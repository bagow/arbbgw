//! SQLite is the authority for order ownership and progress, including across restarts.
use super::orders::{OrderObservation, OrderRequest};
use serde::{Deserialize, Serialize};
use shared::{TradeSignal, TradingSettings};
use tokio_rusqlite::{params, Connection};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Plan {
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
              CREATE INDEX IF NOT EXISTS execution_orders_trade ON execution_orders(trade_id);
              CREATE TABLE IF NOT EXISTS reconciliation_alerts(key TEXT PRIMARY KEY, observed_ms INTEGER NOT NULL, detail TEXT NOT NULL);")?;
            let mut stmt=c.prepare("PRAGMA table_info(trades)")?;
            let cols=stmt.query_map([],|r|r.get::<_,String>(1))?.collect::<Result<Vec<_>,_>>()?;
            drop(stmt);
            for (name,ty) in [("accounting_status","TEXT DEFAULT 'LEGACY_UNVERIFIED'"),("opened_qty_a","REAL"),("opened_qty_b","REAL"),("closed_qty_a","REAL"),("closed_qty_b","REAL"),("manual_close_requested","INTEGER NOT NULL DEFAULT 0"),("peak_net_percent","REAL NOT NULL DEFAULT 0")] {
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
        Ok(self.db.call(move |c| {
            let tx=c.transaction()?;
            let blocked:i64=tx.query_row("SELECT COUNT(*) FROM trades WHERE status NOT IN ('CLOSED','CANCELLED') AND (pair=? OR profile_id=?)",params![s.pair,s.profile_id],|r|r.get(0))?;
            let same:i64=tx.query_row("SELECT COUNT(*) FROM trades WHERE status NOT IN ('CLOSED','CANCELLED') AND pair=?",[&s.pair],|r|r.get(0))?;
            let unresolved:i64=tx.query_row("SELECT COUNT(*) FROM trades WHERE status IN ('ENTRY','RECONCILING')",[],|r|r.get(0))?;
            if same>0 || blocked>=max || unresolved>0 {return Ok(None);}
            tx.execute("INSERT INTO trades(pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,entry_spread,entry_time,status,profile_id,structural_spread,accounting_status,commission_cost,funding_cost,highest_spread_reached) VALUES(?,?,?,?,?,0,0,?,?,'ENTRY',?,?,'PENDING',0,0,0)",params![s.pair,s.exchange_a,s.exchange_b,s.direction,s.actual_vol_a_usd,s.spread,now,s.profile_id,s.structural_spread])?;
            let id=tx.last_insert_rowid();tx.execute("INSERT INTO execution_plans(trade_id,plan) VALUES(?,?)",params![id,json])?;
            tx.commit()?;Ok(Some(id))
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
    pub async fn state(&self, id: i64, state: &str, reason: &str) -> anyhow::Result<()> {
        let (state, reason) = (state.to_string(), reason.to_string());
        self.db.call(move |c|{c.execute("UPDATE trades SET status=?,close_reason=?,close_error=NULL WHERE id=? AND status NOT IN ('CLOSED','CANCELLED')",params![state,reason,id])?;Ok(())}).await?;
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
            c.execute("UPDATE trades SET last_price_a=?,last_price_b=?,curr_gross_pnl=?,peak_net_percent=MAX(peak_net_percent,?) WHERE id=?",params![pa,pb,gross,net_percent,id])?;
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
        self.db.call(move |c| {
            c.execute("UPDATE trades SET status=?,entry_price_a=?,entry_price_b=?,exit_price_a=?,exit_price_b=?,opened_qty_a=?,opened_qty_b=?,closed_qty_a=?,closed_qty_b=?,contracts_a=?,contracts_b=?,volume=?,commission_cost=?,gross_pnl=?,pnl_a=?,pnl_b=?,pnl=NULL,accounting_status=?,exit_time=CASE WHEN ? THEN COALESCE(exit_time,?) ELSE exit_time END WHERE id=?",params![status,a.entry_price(),b.entry_price(),a.exit_price(),b.exit_price(),a.opened,b.opened,a.closed,b.closed,a.opened/multa,b.opened/multb,a.entry_value.max(b.entry_value),fees,gross,if flat {Some(pa)}else{None},if flat {Some(pb)}else{None},accounting,flat,now,id])?;Ok(())
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
    async fn reservation_uses_real_schema_and_blocks_unresolved_entries() {
        let j = setup().await;
        let id = j.reserve(plan()).await.unwrap().unwrap();
        assert_eq!(j.active().await.unwrap()[0].id, id);
        assert!(j.reserve(plan()).await.unwrap().is_none());
        j.state(id, "CANCELLED", "NO_ORDER").await.unwrap();
        assert!(j.reserve(plan()).await.unwrap().is_some());
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
}
