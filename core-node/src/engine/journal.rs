//! SQLite is the authority for order ownership and progress, including across restarts.
use super::orders::{OrderObservation, OrderRequest};
use serde::{Deserialize, Serialize};
use shared::{TradeSignal, TradingSettings};
use tokio_rusqlite::{params, Connection};

pub const PAIR_LOSS_QUARANTINE_MS: i64 = 86_400_000;
pub const PAIR_LOSS_QUARANTINE_THRESHOLD: i64 = 2;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Plan {
    #[serde(default)]
    pub funding_deadline_ms: Option<i64>,
    pub signal: TradeSignal,
    pub settings: TradingSettings,
}
#[derive(Clone, Debug)]
pub struct ManagedTrade {
    pub id: i64,
    pub state: String,
    pub reason: Option<String>,
    pub created_ms: i64,
    pub plan: Plan,
}
#[derive(Clone, Debug)]
pub struct JournalOrder {
    pub trade_id: i64,
    pub leg: String,
    pub exchange: String,
    pub request: OrderRequest,
    pub observation: OrderObservation,
}
#[derive(Clone)]
pub struct Journal {
    pub db: Connection,
}
impl Journal {
    pub async fn init(&self) -> anyhow::Result<()> {
        self.db.call(|c| {
            c.execute_batch("PRAGMA synchronous=FULL;
              CREATE TABLE IF NOT EXISTS execution_plans(trade_id INTEGER PRIMARY KEY, plan TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS execution_orders(client_id TEXT PRIMARY KEY, trade_id INTEGER NOT NULL,
                leg TEXT NOT NULL, exchange TEXT NOT NULL, request TEXT NOT NULL, observation TEXT NOT NULL,
                created_ms INTEGER NOT NULL, updated_ms INTEGER NOT NULL);
              CREATE TABLE IF NOT EXISTS execution_observations(id INTEGER PRIMARY KEY, client_id TEXT NOT NULL,
                observed_ms INTEGER NOT NULL, observation TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS execution_events(id INTEGER PRIMARY KEY, client_id TEXT NOT NULL,
                trade_id INTEGER NOT NULL, event TEXT NOT NULL, at_ms INTEGER NOT NULL, detail TEXT);
              CREATE INDEX IF NOT EXISTS execution_events_trade ON execution_events(trade_id,at_ms);
              CREATE INDEX IF NOT EXISTS execution_orders_trade ON execution_orders(trade_id);
              CREATE TABLE IF NOT EXISTS funding_events(exchange TEXT NOT NULL,event_id TEXT NOT NULL,trade_id INTEGER NOT NULL,time_ms INTEGER NOT NULL,cost REAL NOT NULL,PRIMARY KEY(exchange,event_id));
              CREATE TABLE IF NOT EXISTS reconciliation_alerts(key TEXT PRIMARY KEY, observed_ms INTEGER NOT NULL, detail TEXT NOT NULL);")?;
            let mut stmt=c.prepare("PRAGMA table_info(trades)")?;
            let cols=stmt.query_map([],|r|r.get::<_,String>(1))?.collect::<Result<Vec<_>,_>>()?;
            drop(stmt);
            for (name,ty) in [("accounting_status","TEXT DEFAULT 'LEGACY_UNVERIFIED'"),("opened_qty_a","REAL"),("opened_qty_b","REAL"),("closed_qty_a","REAL"),("closed_qty_b","REAL"),("manual_close_requested","INTEGER NOT NULL DEFAULT 0"),("peak_net_percent","REAL NOT NULL DEFAULT 0"),("last_accounting_check","INTEGER NOT NULL DEFAULT 0"),("accounting_error","TEXT"),("fee_a","REAL"),("fee_b","REAL"),("net_pnl_a","REAL"),("net_pnl_b","REAL"),("current_net_estimate","REAL"),("valuation_ms","INTEGER")] {
                if !cols.iter().any(|x|x==name) { c.execute_batch(&format!("ALTER TABLE trades ADD COLUMN {name} {ty}"))?; }
            }
            Ok(())
        }).await?;
        Ok(())
    }
    pub async fn reserve(&self, plan: Plan) -> anyhow::Result<Option<i64>> {
        let json = serde_json::to_string(&plan)?;
        let s = plan.signal;
        let now = super::utils::now_ms();
        let max = plan.settings.max_active_trades;
        let daily_limit = plan.settings.daily_loss_limit_usdt;
        let exposure_limit = plan.settings.max_total_exposure_usdt;
        let day_start = now - now.rem_euclid(86_400_000);
        let pair_loss_start = now - PAIR_LOSS_QUARANTINE_MS;
        Ok(self.db.call(move |c| {
            let tx=c.transaction()?;
            let blocked:i64=tx.query_row("SELECT COUNT(*) FROM trades WHERE status NOT IN ('CLOSED','CANCELLED') AND (pair=? OR profile_id=?)",params![s.pair,s.profile_id],|r|r.get(0))?;
            let same:i64=tx.query_row("SELECT COUNT(*) FROM trades WHERE status NOT IN ('CLOSED','CANCELLED') AND pair=?",[&s.pair],|r|r.get(0))?;
            let unresolved:i64=tx.query_row("SELECT COUNT(*) FROM trades WHERE status IN ('ENTRY','RECONCILING')",[],|r|r.get(0))?;
            let active_exposure:f64=tx.query_row("SELECT COALESCE(SUM(volume),0) FROM trades WHERE status NOT IN ('CLOSED','CANCELLED') AND profile_id=?",[&s.profile_id],|r|r.get(0))?;
            let daily_pnl:f64=tx.query_row("SELECT COALESCE(SUM(pnl),0) FROM trades WHERE status='CLOSED' AND accounting_status='VERIFIED' AND profile_id=? AND exit_time>=?",params![s.profile_id,day_start],|r|r.get(0))?;
            let recent_pair_losses:i64=tx.query_row("SELECT COUNT(*) FROM trades WHERE status='CLOSED' AND accounting_status='VERIFIED' AND pair=? AND pnl<0 AND exit_time>=?",params![s.pair,pair_loss_start],|r|r.get(0))?;
            let unsettled:i64=tx.query_row("SELECT COUNT(*) FROM trades t JOIN execution_plans p ON p.trade_id=t.id WHERE t.status='CLOSED' AND t.accounting_status!='VERIFIED' AND t.profile_id=?",[&s.profile_id],|r|r.get(0))?;
            let loss_blocked=daily_limit>0.0 && daily_pnl<=-daily_limit;
            let pair_loss_blocked=recent_pair_losses>=PAIR_LOSS_QUARANTINE_THRESHOLD;
            let exposure_blocked=exposure_limit>0.0 && active_exposure+s.actual_vol_a_usd>exposure_limit+1e-9;
            if same>0 || blocked>=max || unresolved>0 || unsettled>0 || loss_blocked || pair_loss_blocked || exposure_blocked {return Ok(None);}
            tx.execute("INSERT INTO trades(pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,entry_spread,entry_time,status,profile_id,structural_spread,accounting_status,commission_cost,funding_cost,highest_spread_reached) VALUES(?,?,?,?,?,0,0,?,?,'ENTRY',?,?,'PENDING',0,0,0)",params![s.pair,s.exchange_a,s.exchange_b,s.direction,s.actual_vol_a_usd,s.spread,now,s.profile_id,s.structural_spread])?;
            let id=tx.last_insert_rowid();tx.execute("INSERT INTO execution_plans(trade_id,plan) VALUES(?,?)",params![id,json])?;
            tx.commit()?;Ok(Some(id))
        }).await?)
    }
    pub async fn daily_verified_pnl(&self, profile_id: &str) -> anyhow::Result<f64> {
        let profile_id = profile_id.to_string();
        let now = super::utils::now_ms();
        let day_start = now - now.rem_euclid(86_400_000);
        Ok(self.db.call(move |c| {
            c.query_row("SELECT COALESCE(SUM(pnl),0) FROM trades WHERE status='CLOSED' AND accounting_status='VERIFIED' AND profile_id=? AND exit_time>=?",params![profile_id,day_start],|r|r.get(0)).map_err(Into::into)
        }).await?)
    }
    pub async fn recent_verified_pair_losses(&self, pair: &str) -> anyhow::Result<i64> {
        let pair = pair.to_string();
        let since = super::utils::now_ms() - PAIR_LOSS_QUARANTINE_MS;
        Ok(self.db.call(move |c| {
            c.query_row("SELECT COUNT(*) FROM trades WHERE status='CLOSED' AND accounting_status='VERIFIED' AND pair=? AND pnl<0 AND exit_time>=?",params![pair,since],|r|r.get(0)).map_err(Into::into)
        }).await?)
    }
    pub async fn active(&self) -> anyhow::Result<Vec<ManagedTrade>> {
        let rows=self.db.call(|c| {
            let mut s=c.prepare("SELECT t.id,t.status,t.close_reason,t.entry_time,p.plan FROM trades t JOIN execution_plans p ON p.trade_id=t.id WHERE t.status NOT IN ('CLOSED','CANCELLED') ORDER BY t.id")?;
            let rows=s.query_map([],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,Option<String>>(2)?,r.get::<_,i64>(3)?,r.get::<_,String>(4)?)))?.collect::<Result<Vec<_>,_>>()?;Ok(rows)
        }).await?;
        rows.into_iter()
            .map(|(id, state, reason, created_ms, p)| {
                Ok(ManagedTrade {
                    id,
                    state,
                    reason,
                    created_ms,
                    plan: serde_json::from_str(&p)?,
                })
            })
            .collect()
    }
    pub async fn orders(&self, id: i64) -> anyhow::Result<Vec<JournalOrder>> {
        let rows=self.db.call(move |c| {
            let mut s=c.prepare("SELECT leg,exchange,request,observation FROM execution_orders WHERE trade_id=? ORDER BY created_ms,client_id")?;
            let rows=s.query_map([id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?)))?.collect::<Result<Vec<_>,_>>()?;Ok(rows)
        }).await?;
        rows.into_iter()
            .map(|(leg, exchange, r, o)| {
                Ok(JournalOrder {
                    trade_id: id,
                    leg,
                    exchange,
                    request: serde_json::from_str(&r)?,
                    observation: serde_json::from_str(&o)?,
                })
            })
            .collect()
    }
    pub async fn prepare(&self, o: &JournalOrder) -> anyhow::Result<()> {
        o.request.validate().map_err(anyhow::Error::msg)?;
        let o = o.clone();
        let r = serde_json::to_string(&o.request)?;
        let ob = serde_json::to_string(&o.observation)?;
        let now = super::utils::now_ms();
        self.db.call(move |c| {c.execute("INSERT INTO execution_orders(client_id,trade_id,leg,exchange,request,observation,created_ms,updated_ms) VALUES(?,?,?,?,?,?,?,?)",params![o.request.client_id,o.trade_id,o.leg,o.exchange,r,ob,now,now])?;Ok(())}).await?;
        Ok(())
    }
    pub async fn observe(&self, o: &JournalOrder, obs: OrderObservation) -> anyhow::Result<()> {
        obs.validate(&o.request).map_err(anyhow::Error::msg)?;
        let id = o.request.client_id.clone();
        let encoded = serde_json::to_string(&obs)?;
        let now = super::utils::now_ms();
        self.db.call(move |c| {
            let old:String=c.query_row("SELECT observation FROM execution_orders WHERE client_id=?",[&id],|r|r.get(0))?;
            // Parse errors are surfaced rather than silently replacing the authority.
            let prev:OrderObservation=serde_json::from_str(&old).map_err(|e|tokio_rusqlite::Error::Other(Box::new(e)))?;
            if obs.filled_quantity+1e-10<prev.filled_quantity
                || (prev.state.terminal() && obs.state != prev.state)
                || (prev.order_id.is_some() && obs.order_id != prev.order_id)
                || (prev.state.terminal() && (obs.filled_quantity-prev.filled_quantity).abs()>1e-10) {
                return Err(tokio_rusqlite::Error::Other("Regressive execution update".into()));
            }
            let tx=c.transaction()?;
            tx.execute("UPDATE execution_orders SET observation=?,updated_ms=? WHERE client_id=?",params![encoded,now,id])?;
            tx.execute("INSERT INTO execution_observations(client_id,observed_ms,observation) VALUES(?,?,?)",params![id,now,encoded])?;
            tx.commit()?;Ok(())
        }).await?;
        Ok(())
    }
    pub async fn execution_event(&self, trade_id: i64, client_id: &str, event: &str, detail: Option<String>) -> anyhow::Result<()> {
        let client_id = client_id.to_string();
        let event = event.to_string();
        let at_ms = super::utils::now_ms();
        self.db.call(move |c| {
            c.execute("INSERT INTO execution_events(client_id,trade_id,event,at_ms,detail) VALUES(?,?,?,?,?)",params![client_id,trade_id,event,at_ms,detail])?;
            Ok(())
        }).await?;
        Ok(())
    }
    pub async fn state(&self, id: i64, state: &str, reason: &str) -> anyhow::Result<()> {
        let (state, reason) = (state.to_string(), reason.to_string());
        self.db.call(move |c|{c.execute("UPDATE trades SET status=?,close_reason=?,close_error=NULL WHERE id=? AND status NOT IN ('CLOSED','CANCELLED')",params![state,reason,id])?;Ok(())}).await?;
        Ok(())
    }
    pub async fn accounting_due(&self) -> anyhow::Result<Vec<(ManagedTrade, i64)>> {
        let now = super::utils::now_ms();
        let rows=self.db.call(move |c| {
            let mut stmt=c.prepare("SELECT t.id,t.close_reason,t.entry_time,t.exit_time,p.plan FROM trades t JOIN execution_plans p ON p.trade_id=t.id WHERE t.status='CLOSED' AND t.exit_time<? AND (t.accounting_status!='VERIFIED' OR t.exit_time>?) AND t.last_accounting_check<? ORDER BY t.last_accounting_check,t.id LIMIT 10")?;
            let rows=stmt.query_map(params![now-60_000,now-7*86_400_000,now-300_000],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,Option<String>>(1)?,r.get::<_,i64>(2)?,r.get::<_,i64>(3)?,r.get::<_,String>(4)?)))?.collect::<Result<Vec<_>,_>>()?;Ok(rows)
        }).await?;
        rows.into_iter()
            .map(|(id, reason, created_ms, end, plan)| {
                Ok((
                    ManagedTrade {
                        id,
                        state: "CLOSED".into(),
                        reason,
                        created_ms,
                        plan: serde_json::from_str(&plan)?,
                    },
                    end,
                ))
            })
            .collect()
    }
    pub async fn accounting_attempt(&self, id: i64, error: Option<String>) -> anyhow::Result<()> {
        let now = super::utils::now_ms();
        self.db
            .call(move |c| {
                c.execute(
                    "UPDATE trades SET last_accounting_check=?,accounting_error=? WHERE id=?",
                    params![now, error, id],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }
    pub async fn settle(
        &self,
        id: i64,
        events: Vec<(String, super::orders::FundingEvent)>,
    ) -> anyhow::Result<()> {
        let mut unique = std::collections::BTreeMap::new();
        for (ex, e) in events {
            if e.id.is_empty() || !e.cost.is_finite() {
                anyhow::bail!("Invalid funding evidence");
            }
            if let Some(previous) = unique.insert((ex, e.id.clone()), (e.time_ms, e.cost)) {
                if previous != (e.time_ms, e.cost) {
                    anyhow::bail!("Conflicting funding event");
                }
            }
        }
        self.db.call(move |c| {
            let tx=c.transaction()?;
            for ((ex,event_id),(time_ms,cost)) in &unique {
                let owner=tx.query_row("SELECT trade_id FROM funding_events WHERE exchange=? AND event_id=?",params![ex,event_id],|r|r.get::<_,i64>(0));
                match owner {Ok(other) if other!=id=>return Err(tokio_rusqlite::Error::Other("Funding already belongs to another trade".into())),Err(rusqlite::Error::QueryReturnedNoRows)|Ok(_)=>{},Err(e)=>return Err(e.into())}
                tx.execute("INSERT INTO funding_events(exchange,event_id,trade_id,time_ms,cost) VALUES(?,?,?,?,?) ON CONFLICT(exchange,event_id) DO UPDATE SET cost=excluded.cost,time_ms=excluded.time_ms",params![ex,event_id,id,time_ms,cost])?;
            }
            let stored:i64=tx.query_row("SELECT COUNT(*) FROM funding_events WHERE trade_id=?",[id],|r|r.get(0))?;
            if stored!=unique.len() as i64{return Err(tokio_rusqlite::Error::Other("Funding ledger lost previously observed events".into()));}
            let funding=unique.values().map(|(_,cost)|cost).sum::<f64>();
            let (exa,exb):(String,String)=tx.query_row("SELECT exchange_a,exchange_b FROM trades WHERE id=?",[id],|r|Ok((r.get(0)?,r.get(1)?)))?;
            let fa=unique.iter().filter(|((ex,_),_)|*ex==exa).map(|(_,(_,cost))|cost).sum::<f64>();
            let fb=unique.iter().filter(|((ex,_),_)|*ex==exb).map(|(_,(_,cost))|cost).sum::<f64>();
            let updated=tx.execute("UPDATE trades SET funding_cost=?,pnl=gross_pnl-commission_cost-?,accounting_status='VERIFIED',accounting_error=NULL,net_pnl_a=pnl_a-fee_a-?,net_pnl_b=pnl_b-fee_b-? WHERE id=? AND status='CLOSED' AND accounting_status='FUNDING_PENDING' AND gross_pnl IS NOT NULL",params![funding,funding,fa,fb,id])?;
            if updated!=1{return Err(tokio_rusqlite::Error::Other("Trade not ready for accounting settlement".into()));}
            tx.commit()?;Ok(())
        }).await?;
        Ok(())
    }
    pub async fn request_close(&self, id: i64) -> anyhow::Result<()> {
        self.db.call(move |c| {c.execute("UPDATE trades SET manual_close_requested=1 WHERE id=? AND status NOT IN ('CLOSED','CANCELLED')",[id])?;Ok(())}).await?;
        Ok(())
    }
    pub async fn close_requested(&self, id: i64) -> anyhow::Result<bool> {
        Ok(self
            .db
            .call(move |c| {
                c.query_row(
                    "SELECT manual_close_requested FROM trades WHERE id=?",
                    [id],
                    |r| r.get::<_, bool>(0),
                )
                .map_err(Into::into)
            })
            .await?)
    }
    pub async fn mark_to_market(
        &self,
        id: i64,
        pa: f64,
        pb: f64,
        gross: f64,
        net_percent: f64,
    ) -> anyhow::Result<f64> {
        Ok(self.db.call(move |c| {
            c.execute("UPDATE trades SET last_price_a=?,last_price_b=?,curr_gross_pnl=?,peak_net_percent=MAX(peak_net_percent,?),current_net_estimate=?*entry_price_a*opened_qty_a/100.0,valuation_ms=? WHERE id=?",params![pa,pb,gross,net_percent,net_percent,super::utils::now_ms(),id])?;
            c.query_row("SELECT peak_net_percent FROM trades WHERE id=?",[id],|r|r.get(0)).map_err(Into::into)
        }).await?)
    }
    pub async fn alert(&self, key: String, detail: String) -> anyhow::Result<()> {
        let now = super::utils::now_ms();
        self.db.call(move |c|{c.execute("INSERT INTO reconciliation_alerts(key,observed_ms,detail) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET observed_ms=excluded.observed_ms,detail=excluded.detail",params![key,now,detail])?;Ok(())}).await?;
        Ok(())
    }
    pub async fn project(
        &self,
        t: &ManagedTrade,
        orders: &[JournalOrder],
        closing: bool,
    ) -> anyhow::Result<()> {
        let s = &t.plan.signal;
        let a = aggregate(orders, "A");
        let b = aggregate(orders, "B");
        let unresolved = orders.iter().any(|o| !o.observation.state.terminal());
        if a.closed > a.opened + 1e-8 || b.closed > b.opened + 1e-8 {
            anyhow::bail!("Close quantity exceeds owned exposure");
        }
        let flat = a.remaining() <= 1e-9 && b.remaining() <= 1e-9 && !unresolved;
        let filled = a.opened + b.opened > 0.0;
        let status = if closing && flat {
            if filled {
                "CLOSED"
            } else {
                "CANCELLED"
            }
        } else if closing {
            "CLOSING"
        } else {
            "OPEN"
        };
        let confirmed = orders
            .iter()
            .all(|o| o.observation.fee.is_some() || o.observation.filled_quantity == 0.0);
        let fees = a.fee + b.fee;
        let pa = if s.is_a_higher {
            a.entry_value - a.exit_value
        } else {
            a.exit_value - a.entry_value
        };
        let pb = if s.is_a_higher {
            b.exit_value - b.entry_value
        } else {
            b.entry_value - b.exit_value
        };
        let gross = if flat { Some(pa + pb) } else { None };
        // Funding must be reconciled separately. Never advertise an unaudited net as final.
        let accounting = if !filled {
            "VERIFIED"
        } else if confirmed {
            "FUNDING_PENDING"
        } else {
            "FEES_PENDING"
        };
        let (id, multa, multb) = (t.id, s.mult_a, s.mult_b);
        let now = super::utils::now_ms();
        let id_a = orders
            .iter()
            .find(|o| o.leg == "A" && !o.request.reduce_only)
            .and_then(|o| o.observation.order_id.clone());
        let id_b = orders
            .iter()
            .find(|o| o.leg == "B" && !o.request.reduce_only)
            .and_then(|o| o.observation.order_id.clone());
        let spread = if a.entry_price() > 0.0 && b.entry_price() > 0.0 {
            if s.is_a_higher {
                (a.entry_price() / b.entry_price() - 1.0) * 100.0
            } else {
                (b.entry_price() / a.entry_price() - 1.0) * 100.0
            }
        } else {
            0.0
        };
        self.db.call(move |c| {
            let tx=c.transaction()?;
            tx.execute("UPDATE trades SET fee_a=?,fee_b=?,order_id_a=?,order_id_b=?,entry_spread=? WHERE id=?",params![a.fee,b.fee,id_a,id_b,spread,id])?;
            tx.execute("UPDATE trades SET status=?,entry_price_a=?,entry_price_b=?,exit_price_a=?,exit_price_b=?,opened_qty_a=?,opened_qty_b=?,closed_qty_a=?,closed_qty_b=?,contracts_a=?,contracts_b=?,volume=?,commission_cost=?,gross_pnl=?,pnl_a=?,pnl_b=?,pnl=NULL,accounting_status=?,exit_time=CASE WHEN ? THEN COALESCE(exit_time,?) ELSE exit_time END WHERE id=?",params![status,a.entry_price(),b.entry_price(),a.exit_price(),b.exit_price(),a.opened,b.opened,a.closed,b.closed,a.opened/multa,b.opened/multb,a.entry_value.max(b.entry_value),fees,gross,if flat {Some(pa)}else{None},if flat {Some(pb)}else{None},accounting,flat,now,id])?;tx.commit()?;Ok(())
        }).await?;
        Ok(())
    }
}
#[derive(Default, Clone, Debug)]
pub struct LegTotals {
    pub opened: f64,
    pub closed: f64,
    pub entry_value: f64,
    pub exit_value: f64,
    pub fee: f64,
}
impl LegTotals {
    pub fn remaining(&self) -> f64 {
        (self.opened - self.closed).max(0.0)
    }
    pub fn entry_price(&self) -> f64 {
        if self.opened > 0.0 {
            self.entry_value / self.opened
        } else {
            0.0
        }
    }
    pub fn exit_price(&self) -> Option<f64> {
        if self.closed > 0.0 {
            Some(self.exit_value / self.closed)
        } else {
            None
        }
    }
}
pub fn aggregate(orders: &[JournalOrder], leg: &str) -> LegTotals {
    let mut v = LegTotals::default();
    for o in orders.iter().filter(|o| o.leg == leg) {
        let q = o.observation.filled_quantity;
        let value = q * o.observation.average_price.unwrap_or(0.0);
        if o.request.reduce_only {
            v.closed += q;
            v.exit_value += value;
        } else {
            v.opened += q;
            v.entry_value += value;
        }
        v.fee += o.observation.fee.unwrap_or(0.0);
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::orders::OrderState;
    fn plan() -> Plan {
        Plan {
            funding_deadline_ms: None,
            settings: TradingSettings::default(),
            signal: TradeSignal {
                profile_id: "real-default".into(),
                pair: "TESTUSDT".into(),
                exchange_a: "gate".into(),
                exchange_b: "binance".into(),
                direction: "LONG_A_SHORT_B".into(),
                contracts_a: 10.0,
                contracts_b: 10.0,
                target_qty_a: 10.0,
                target_qty_b: 10.0,
                limit_price_a: 1.0,
                limit_price_b: 1.1,
                entry_a: 1.0,
                entry_b: 1.1,
                actual_vol_a_usd: 10.0,
                actual_vol_b_usd: 11.0,
                break_even_pct: 0.2,
                spread: 10.0,
                structural_spread: 0.0,
                is_a_higher: false,
                tick_a: 0.01,
                tick_b: 0.01,
                mult_a: 1.0,
                mult_b: 1.0,
            },
        }
    }
    async fn setup() -> Journal {
        let db = Connection::open_in_memory().await.unwrap();
        crate::engine::startup::init_trades_db(&db).await.unwrap();
        let j = Journal { db };
        j.init().await.unwrap();
        j
    }
    fn order(id: i64) -> JournalOrder {
        JournalOrder {
            trade_id: id,
            leg: "A".into(),
            exchange: "gate".into(),
            request: OrderRequest {
                client_id: "stable-client-id".into(),
                pair: "TESTUSDT".into(),
                is_buy: true,
                quantity: 10.0,
                limit_price: 1.0,
                reduce_only: false,
                multiplier: 1.0,
                tick_size: 0.01,
                leverage: 1.0,
            },
            observation: OrderObservation {
                order_id: None,
                state: OrderState::Unknown,
                filled_quantity: 0.0,
                average_price: None,
                fee: None,
            },
        }
    }
    #[tokio::test]
    async fn restart_preserves_unknown_identity_plan_and_partial_exposure() {
        let path = std::env::temp_dir().join(format!("arb-restart-{}.db", uuid::Uuid::new_v4()));
        let db = Connection::open(&path).await.unwrap();
        crate::engine::startup::init_trades_db(&db).await.unwrap();
        let j = Journal { db };
        j.init().await.unwrap();
        let mut p = plan();
        p.funding_deadline_ms = Some(1_900_000_000_000);
        let id = j.reserve(p).await.unwrap().unwrap();
        let o = order(id);
        j.prepare(&o).await.unwrap();
        // Process is lost after sending, before any acknowledgement is recorded.
        j.db.close().await.unwrap();
        let j = Journal {
            db: Connection::open(&path).await.unwrap(),
        };
        j.init().await.unwrap();
        let recovered = j.active().await.unwrap().remove(0);
        assert_eq!(recovered.plan.funding_deadline_ms, Some(1_900_000_000_000));
        assert_eq!(
            j.orders(id).await.unwrap()[0].observation.state,
            OrderState::Unknown
        );
        assert!(j.prepare(&o).await.is_err());
        let fill = OrderObservation {
            order_id: Some("accepted-before-crash".into()),
            state: OrderState::Cancelled,
            filled_quantity: 3.0,
            average_price: Some(1.0),
            fee: None,
        };
        j.observe(&o, fill.clone()).await.unwrap();
        j.observe(&o, fill).await.unwrap();
        j.project(&recovered, &j.orders(id).await.unwrap(), true)
            .await
            .unwrap();
        j.db.close().await.unwrap();
        let j = Journal {
            db: Connection::open(&path).await.unwrap(),
        };
        j.init().await.unwrap();
        assert_eq!(j.active().await.unwrap()[0].state, "CLOSING");
        assert_eq!(
            aggregate(&j.orders(id).await.unwrap(), "A").remaining(),
            3.0
        );
        j.db.close().await.unwrap();
        std::fs::remove_file(path).unwrap();
    }
    #[tokio::test]
    async fn reservation_uses_real_schema_and_blocks_unresolved_entries() {
        let j = setup().await;
        let id = j.reserve(plan()).await.unwrap().unwrap();
        assert_eq!(j.active().await.unwrap()[0].id, id);
        assert!(j.reserve(plan()).await.unwrap().is_none());
        j.state(id, "CANCELLED", "NO_ORDER").await.unwrap();
        assert!(j.reserve(plan()).await.unwrap().is_some());
    }
    #[tokio::test]
    async fn persistent_money_limits_block_new_reservations() {
        let j = setup().await;
        let mut too_large = plan();
        too_large.settings.max_total_exposure_usdt = 5.0;
        assert!(j.reserve(too_large).await.unwrap().is_none());

        let id = j.reserve(plan()).await.unwrap().unwrap();
        let now = super::super::utils::now_ms();
        j.db.call(move |c| {
            c.execute("UPDATE trades SET status='CLOSED',accounting_status='VERIFIED',pnl=-1.0,exit_time=? WHERE id=?",params![now,id])?;
            Ok(())
        }).await.unwrap();
        let mut after_loss = plan();
        after_loss.settings.daily_loss_limit_usdt = 0.5;
        assert!(j.reserve(after_loss).await.unwrap().is_none());
    }
    #[tokio::test]
    async fn repeated_verified_pair_losses_are_quarantined_for_a_day() {
        let j = setup().await;
        let now = super::super::utils::now_ms();
        let first = j.reserve(plan()).await.unwrap().unwrap();
        j.db.call(move |c| {
            c.execute("UPDATE trades SET status='CLOSED',accounting_status='VERIFIED',pnl=-0.01,exit_time=? WHERE id=?",params![now,first])?;
            Ok(())
        }).await.unwrap();
        assert_eq!(j.recent_verified_pair_losses("TESTUSDT").await.unwrap(), 1);

        let second = j.reserve(plan()).await.unwrap().unwrap();
        j.db.call(move |c| {
            c.execute("UPDATE trades SET status='CLOSED',accounting_status='VERIFIED',pnl=-0.01,exit_time=? WHERE id=?",params![now,second])?;
            Ok(())
        }).await.unwrap();
        assert_eq!(j.recent_verified_pair_losses("TESTUSDT").await.unwrap(), 2);
        assert!(j.reserve(plan()).await.unwrap().is_none());
    }
    #[tokio::test]
    async fn durable_identity_and_cumulative_receipts_cannot_double_count() {
        let j = setup().await;
        let id = j.reserve(plan()).await.unwrap().unwrap();
        let o = order(id);
        j.prepare(&o).await.unwrap();
        assert!(j.prepare(&o).await.is_err());
        let partial = OrderObservation {
            order_id: Some("exchange-id".into()),
            state: OrderState::PartiallyFilled,
            filled_quantity: 3.0,
            average_price: Some(1.0),
            fee: Some(0.01),
        };
        j.observe(&o, partial.clone()).await.unwrap();
        j.observe(&o, partial.clone()).await.unwrap();
        assert_eq!(aggregate(&j.orders(id).await.unwrap(), "A").opened, 3.0);
        let mut cancelled = partial.clone();
        cancelled.state = OrderState::Cancelled;
        j.observe(&o, cancelled).await.unwrap();
        assert!(j.observe(&o, partial).await.is_err());
        let persisted = j.orders(id).await.unwrap();
        assert_eq!(persisted[0].observation.state, OrderState::Cancelled);
        assert_eq!(aggregate(&persisted, "A").fee, 0.01);
    }
    #[tokio::test]
    async fn partial_close_cannot_be_reported_flat() {
        let j = setup().await;
        let id = j.reserve(plan()).await.unwrap().unwrap();
        let mut o = order(id);
        o.observation = OrderObservation {
            order_id: Some("entry".into()),
            state: OrderState::Filled,
            filled_quantity: 10.0,
            average_price: Some(1.0),
            fee: Some(0.01),
        };
        let mut close = o.clone();
        close.request.reduce_only = true;
        close.request.client_id = "close".into();
        close.observation.state = OrderState::Cancelled;
        close.observation.filled_quantity = 4.0;
        let t = j.active().await.unwrap().remove(0);
        j.project(&t, &[o, close], true).await.unwrap();
        assert_eq!(j.active().await.unwrap()[0].state, "CLOSING");
        let net: Option<f64> =
            j.db.call(move |c| {
                c.query_row("SELECT pnl FROM trades WHERE id=?", [id], |r| r.get(0))
                    .map_err(Into::into)
            })
            .await
            .unwrap();
        assert!(net.is_none());
    }
    #[tokio::test]
    async fn manual_close_request_survives_projection() {
        let j = setup().await;
        let id = j.reserve(plan()).await.unwrap().unwrap();
        j.request_close(id).await.unwrap();
        j.state(id, "OPEN", "").await.unwrap();
        assert!(j.close_requested(id).await.unwrap());
        assert_eq!(j.mark_to_market(id, 1.0, 1.0, 0.3, 3.0).await.unwrap(), 3.0);
        assert_eq!(j.mark_to_market(id, 1.0, 1.0, 0.1, 1.0).await.unwrap(), 3.0);
    }
    #[tokio::test]
    async fn funding_reconciliation_is_idempotent_and_cannot_double_assign_events() {
        let j = setup().await;
        let id = j.reserve(plan()).await.unwrap().unwrap();
        let t = j.active().await.unwrap().remove(0);
        let mut entry = order(id);
        entry.observation = OrderObservation {
            order_id: Some("entry".into()),
            state: OrderState::Filled,
            filled_quantity: 10.0,
            average_price: Some(1.0),
            fee: Some(0.01),
        };
        let mut close = entry.clone();
        close.request.reduce_only = true;
        close.request.client_id = "exit".into();
        close.observation.order_id = Some("exit".into());
        let orders = vec![entry, close];
        let event = (
            "gate".to_string(),
            super::super::orders::FundingEvent {
                id: "funding-1".into(),
                time_ms: t.created_ms + 1,
                cost: 0.005,
            },
        );
        for _ in 0..2 {
            j.project(&t, &orders, true).await.unwrap();
            j.settle(id, vec![event.clone(), event.clone()])
                .await
                .unwrap();
        }
        let (net, count): (f64, i64) =
            j.db.call(move |c| {
                Ok((
                    c.query_row("SELECT pnl FROM trades WHERE id=?", [id], |r| r.get(0))?,
                    c.query_row("SELECT COUNT(*) FROM funding_events", [], |r| r.get(0))?,
                ))
            })
            .await
            .unwrap();
        assert!((net + 0.025).abs() < 1e-10);
        assert_eq!(count, 1);
        let second = j.reserve(plan()).await.unwrap().unwrap();
        assert!(j.settle(second, vec![event]).await.is_err());
    }
}
