use tokio_rusqlite::{params, Connection};
use tracing::info;

use crate::engine::utils::now_ms;
use crate::engine::real::Trade;

#[derive(Clone)]
pub struct TradeRepository {
    pub db: Connection,
}

impl TradeRepository {
    pub fn new(db: Connection) -> Self {
        Self { db }
    }

    pub async fn insert_snapshot(
        &self,
        pair: String,
        exa: String,
        exb: String,
        snapshot_json: String,
        timestamp: i64,
    ) {
        let _ = self.db.call(move |conn| {
            conn.execute(
                "INSERT INTO decision_snapshots (timestamp, pair, exchange_a, exchange_b, snapshot_json) VALUES (?,?,?,?,?)",
                params![timestamp, pair, exa, exb, snapshot_json]
            )?;
            Ok(())
        }).await;
    }

    pub async fn load_open_trades(&self, profile_id: &str) -> Vec<Trade> {
        crate::engine::real::load_open_trades(&self.db, profile_id).await
    }



    pub async fn insert_new_entry(
        &self,
        pair: String,
        exa: String,
        exb: String,
        dir: String,
        vol: f64,
        fill_a: f64,
        fill_b: f64,
        sp: f64,
        entry_time: i64,
        profile_id: String,
        entry_comm: f64,
        entry_slip: f64,
        oid_a_db: String,
        oid_b_db: String,
        contracts_a: f64,
        contracts_b: f64,
        structural_spread: f64,
    ) {
        let _ = self.db.call(move |conn| {
            conn.execute(
                "INSERT INTO trades (pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,entry_spread,entry_time,highest_spread_reached,status,pnl_a,pnl_b,profile_id,commission_cost,slippage_cost,funding_cost,order_id_a,order_id_b,contracts_a,contracts_b,structural_spread) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params![pair, exa, exb, dir, vol, fill_a, fill_b, sp, entry_time, sp, "OPEN", 0.0f64, 0.0f64, profile_id, entry_comm, entry_slip, 0.0f64, oid_a_db, oid_b_db, contracts_a, contracts_b, structural_spread]
            )?;
            Ok(())
        }).await;
    }

    pub async fn update_trade_entry_commission(&self, entry_time: i64, pair: String, commission_cost: f64) {
        let _ = self.db.call(move |conn| {
            conn.execute(
                "UPDATE trades SET commission_cost = ? WHERE entry_time = ? AND pair = ?",
                params![commission_cost, entry_time, pair],
            )?;
            Ok(())
        }).await;
    }

    pub async fn append_asymmetric_close(
        &self,
        fp: String,
        ea_s: String,
        eb_s: String,
        d: String,
        fv: f64,
        ent_a: f64,
        ent_b: f64,
        sp: f64,
        entry_time: i64,
        pnl_a_val: f64,
        pnl_b_val: f64,
        pid: String,
        total_fee: f64,
        slip: f64,
        net_pnl: f64,
        gross_pnl: f64,
        ext_a: f64,
        ext_b: f64,
    ) {
        let _ = self.db.call(move |conn| {
            conn.execute(
                "INSERT INTO trades (pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,entry_spread,entry_time,exit_time,highest_spread_reached,status,pnl_a,pnl_b,profile_id,commission_cost,slippage_cost,funding_cost,close_reason,pnl,gross_pnl,exit_price_a,exit_price_b) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params![fp, ea_s, eb_s, d, fv, ent_a, ent_b, sp, entry_time, entry_time, sp, "CLOSED", pnl_a_val, pnl_b_val, pid, total_fee, slip, 0.0f64, "ASYMMETRIC_CLOSE", net_pnl, gross_pnl, ext_a, ext_b]
            )?;
            Ok(())
        }).await;
    }

    pub async fn update_highest_spread(&self, id: i64, top_spread: f64) {
        let _ = self
            .db
            .call(move |conn| {
                conn.execute(
                    "UPDATE trades SET highest_spread_reached=? WHERE id=?",
                    params![top_spread, id],
                )?;
                Ok(())
            })
            .await;
    }

    /// Update open trade live state in a single batched transaction.
    /// Persists real-time prices, net PnL, per-leg PnL, real funding rates,
    /// and the accumulated funding cost.
    pub async fn flush_live_updates(
        &self,
        live_updates: Vec<(i64, f64, f64, f64, f64, f64, f64, i64, f64, f64, f64)>,
    ) {
        let _ = self
            .db
            .call(move |conn| {
                let tx = conn.transaction()?;
                {
                    let mut stmt = tx.prepare_cached(
                        "UPDATE trades SET \
                     last_price_a=?, last_price_b=?, last_spread=?, curr_gross_pnl=?, \
                     funding_rate_a=?, funding_rate_b=?, funding_next_ts=?, \
                     funding_cost=?, pnl_a=?, pnl_b=? \
                     WHERE id=? AND status IN ('OPEN','CLOSING')",
                    )?;
                    for (id, pa, pb, cs, pnl, fra, frb, fnt, fund_cost, pnl_a, pnl_b) in
                        live_updates
                    {
                        stmt.execute(params![
                            pa, pb, cs, pnl, fra, frb, fnt, fund_cost, pnl_a, pnl_b, id
                        ])?;
                    }
                }
                tx.commit()?;
                Ok(())
            })
            .await;
    }

    #[allow(dead_code)]
    pub async fn correct_entry_prices(
        &self,
        oid_a2: String,
        oid_b2: String,
        real_a: f64,
        real_b: f64,
        total_fee: f64,
    ) {
        let _ = self
            .db
            .call(move |conn| {
                if real_a > 0.0 {
                    let _ = conn.execute(
                        "UPDATE trades SET entry_price_a=? WHERE order_id_a=? AND status IN ('OPEN','CLOSING')",
                        params![real_a, oid_a2],
                    );
                }
                if real_b > 0.0 {
                    let _ = conn.execute(
                        "UPDATE trades SET entry_price_b=? WHERE order_id_b=? AND status IN ('OPEN','CLOSING')",
                        params![real_b, oid_b2],
                    );
                }
                if total_fee > 1e-9 {
                    let _ = conn.execute(
                        "UPDATE trades SET commission_cost=? WHERE order_id_a=? AND status IN ('OPEN','CLOSING')",
                        params![total_fee, oid_a2],
                    );
                }
                Ok(())
            })
            .await;
    }

    /// Revert a trade back to OPEN state when a close order fails to fill.
    /// Clears close_reason so the engine can re-trigger on the next cycle.
    #[allow(dead_code)]
    pub async fn revert_to_open(&self, id: i64) {
        let _ = self
            .db
            .call(move |conn| {
                conn.execute(
                    "UPDATE trades SET status='OPEN', close_reason=NULL WHERE id=? AND status IN ('OPEN','CLOSING')",
                    params![id],
                )?;
                Ok(())
            })
            .await;
    }

    pub async fn begin_close_attempt(&self, id: i64, reason: &str) -> bool {
        let reason_owned = reason.to_string();
        let ts = now_ms();
        self.db
            .call(move |conn| {
                let changed = conn.execute(
                    "UPDATE trades SET \
                     status='CLOSING', \
                     close_reason=CASE WHEN close_reason IS NULL OR close_reason='' THEN ? ELSE close_reason END, \
                     close_attempts=COALESCE(close_attempts,0)+1, \
                     last_close_attempt_at=?, \
                     close_error=NULL \
                     WHERE id=? AND status IN ('OPEN','CLOSING')",
                    params![reason_owned, ts, id],
                )?;
                Ok(changed > 0)
            })
            .await
            .unwrap_or(false)
    }

    pub async fn record_close_failure(&self, id: i64, err: &str, keep_closing: bool) {
        let err_owned = err.to_string();
        let ts = now_ms();
        let _ = self
            .db
            .call(move |conn| {
                if keep_closing {
                    conn.execute(
                        "UPDATE trades SET status='CLOSING', close_error=?, last_close_attempt_at=? \
                         WHERE id=? AND status IN ('OPEN','CLOSING')",
                        params![err_owned, ts, id],
                    )?;
                } else {
                    conn.execute(
                        "UPDATE trades SET status='OPEN', close_reason=NULL, close_error=?, last_close_attempt_at=? \
                         WHERE id=? AND status IN ('OPEN','CLOSING')",
                        params![err_owned, ts, id],
                    )?;
                }
                Ok(())
            })
            .await;
    }

    /// Atomically mark trade as CLOSED with real fill prices.
    /// Called ONLY after fill verification succeeds — never speculatively.
    pub async fn mark_trade_closed_confirmed(
        &self,
        id: i64,
        exit_price_a: f64,
        exit_price_b: f64,
        exit_spread: f64,
        gross_pnl: f64,
        pnl_a: f64,
        pnl_b: f64,
        reason: &str,
        fund_cost: f64,
        exit_fee: f64,
        exit_slip: f64,
    ) {
        let r = reason.to_string();
        let r_clone = r.clone();
        let current_ms = now_ms();
        // Compute net PnL here from known parameter values, NOT from stale DB column values.
        let _ = self.db.call(move |conn| {
            // Read entry commission to include in net pnl
            let entry_comm: f64 = conn.query_row(
                "SELECT COALESCE(commission_cost, 0.0) FROM trades WHERE id=?",
                params![id],
                |row| row.get(0)
            ).unwrap_or(0.0);

            let total_comm = entry_comm + exit_fee;
            // Net PnL: gross already uses actual fill prices (slippage is baked in).
            // Slippage fields are diagnostic only — NOT subtracted from PnL.
            let net_pnl = gross_pnl - total_comm - fund_cost;

            conn.execute(
                "UPDATE trades SET \
                 status='CLOSED', exit_price_a=?, exit_price_b=?, exit_spread=?, exit_time=?, \
                 pnl_a=?, pnl_b=?, gross_pnl=?, curr_gross_pnl=?, close_reason=?, \
                 funding_cost=?, commission_cost=?, exit_slippage_cost=?, close_error=NULL, \
                 pnl=? \
                 WHERE id=?",
                params![
                    exit_price_a, exit_price_b, exit_spread, current_ms,
                    pnl_a, pnl_b, gross_pnl, gross_pnl, r_clone,
                    fund_cost, total_comm, exit_slip,
                    net_pnl,
                    id
                ]
            )?;
            Ok(())
        }).await;
        info!(
            "[db] CLOSED CONFIRMED id={} reason={} gross={:.4} fee={:.4} exit_slip={:.4} fund={:.4}",
            id, r, gross_pnl, exit_fee, exit_slip, fund_cost
        );
    }

    pub async fn emergency_close_trade(&self, id: i64) {
        let _ = self.db.call(move |conn| {
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
            conn.execute(
                "UPDATE trades SET status='CLOSED', exit_time=?, close_reason='EXTERNAL_CLOSE', \
                 gross_pnl=COALESCE(pnl_a,0.0)+COALESCE(pnl_b,0.0), \
                 pnl=COALESCE(curr_gross_pnl, \
                     (COALESCE(pnl_a,0.0)+COALESCE(pnl_b,0.0) \
                      -COALESCE(commission_cost,0.0)-COALESCE(funding_cost,0.0))), \
                 curr_gross_pnl=COALESCE(curr_gross_pnl, \
                     (COALESCE(pnl_a,0.0)+COALESCE(pnl_b,0.0) \
                      -COALESCE(commission_cost,0.0)-COALESCE(funding_cost,0.0))) \
                 WHERE id=?",
                tokio_rusqlite::params![now, id]
            )?;
            Ok(())
        }).await;
    }

    pub async fn emergency_partial_close_trade(&self, id: i64, total_fee: f64) {
        let _ = self.db.call(move |conn| {
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
            conn.execute(
                "UPDATE trades SET status='CLOSED', exit_time=?, close_reason='POSITION_SYNC_CLOSE', \
                 commission_cost=?, \
                 gross_pnl=COALESCE(pnl_a,0.0)+COALESCE(pnl_b,0.0), \
                 pnl=(COALESCE(pnl_a,0.0)+COALESCE(pnl_b,0.0) \
                     -?-COALESCE(funding_cost,0.0)), \
                 curr_gross_pnl=(COALESCE(pnl_a,0.0)+COALESCE(pnl_b,0.0) \
                     -?-COALESCE(funding_cost,0.0)) \
                 WHERE id=?",
                tokio_rusqlite::params![now, total_fee, total_fee, total_fee, id]
            )?;
            Ok(())
        }).await;
    }

    /// Asynchronously backfill and update trade record with exact verified exchange numbers
    /// (real commissions paid, real exit prices, real realized PnL, real funding).
    pub async fn apply_trade_audit(
        &self,
        id: i64,
        entry_price_a: Option<f64>,
        entry_price_b: Option<f64>,
        exit_price_a: Option<f64>,
        exit_price_b: Option<f64>,
        commission_cost: f64,
        funding_cost: f64,
        pnl_a: Option<f64>,
        pnl_b: Option<f64>,
        gross_pnl: Option<f64>,
        net_pnl: Option<f64>,
    ) {
        let _ = self.db.call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT entry_price_a, entry_price_b, exit_price_a, exit_price_b, \
                 COALESCE(commission_cost, 0.0), COALESCE(funding_cost, 0.0), \
                 COALESCE(pnl_a, 0.0), COALESCE(pnl_b, 0.0), COALESCE(gross_pnl, 0.0), COALESCE(pnl, 0.0) \
                 FROM trades WHERE id=?"
            )?;
            let current = stmt.query_row(params![id], |r| {
                Ok((
                    r.get::<_, f64>(0)?,
                    r.get::<_, f64>(1)?,
                    r.get::<_, Option<f64>>(2)?,
                    r.get::<_, Option<f64>>(3)?,
                    r.get::<_, f64>(4)?,
                    r.get::<_, f64>(5)?,
                    r.get::<_, f64>(6)?,
                    r.get::<_, f64>(7)?,
                    r.get::<_, f64>(8)?,
                    r.get::<_, f64>(9)?,
                ))
            });

            if let Ok((cur_ent_a, cur_ent_b, cur_ext_a, cur_ext_b, cur_comm, cur_fund, cur_pnl_a, cur_pnl_b, cur_gross, _cur_net)) = current {
                let new_ent_a = entry_price_a.filter(|&p| p > 0.0).unwrap_or(cur_ent_a);
                let new_ent_b = entry_price_b.filter(|&p| p > 0.0).unwrap_or(cur_ent_b);
                let new_ext_a = exit_price_a.filter(|&p| p > 0.0).or(cur_ext_a);
                let new_ext_b = exit_price_b.filter(|&p| p > 0.0).or(cur_ext_b);
                let new_comm = if commission_cost > 0.0 { commission_cost } else { cur_comm };
                let new_fund = if funding_cost != 0.0 { funding_cost } else { cur_fund };
                let new_pnl_a = pnl_a.unwrap_or(cur_pnl_a);
                let new_pnl_b = pnl_b.unwrap_or(cur_pnl_b);
                let new_gross = gross_pnl.unwrap_or_else(|| {
                    if pnl_a.is_some() || pnl_b.is_some() {
                        new_pnl_a + new_pnl_b
                    } else {
                        cur_gross
                    }
                });
                let new_net = net_pnl.unwrap_or(new_gross - new_comm - new_fund);

                conn.execute(
                    "UPDATE trades SET \
                     entry_price_a=?, entry_price_b=?, exit_price_a=?, exit_price_b=?, \
                     commission_cost=?, funding_cost=?, pnl_a=?, pnl_b=?, \
                     gross_pnl=?, curr_gross_pnl=?, pnl=? \
                     WHERE id=?",
                    params![
                        new_ent_a, new_ent_b, new_ext_a, new_ext_b,
                        new_comm, new_fund, new_pnl_a, new_pnl_b,
                        new_gross, new_gross, new_net,
                        id
                    ]
                )?;
            }
            Ok(())
        }).await;
    }
}
