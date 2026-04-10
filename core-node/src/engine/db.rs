use tokio_rusqlite::{Connection, params};
use tracing::{info, error};

use crate::engine::real::Trade;
use crate::engine::real::now_ms;

#[derive(Clone)]
pub struct TradeRepository {
    pub db: Connection,
}

impl TradeRepository {
    pub fn new(db: Connection) -> Self {
        Self { db }
    }

    pub async fn load_open_trades(&self, profile_id: &str) -> Vec<Trade> {
        crate::engine::real::load_open_trades(&self.db, profile_id).await
    }

    pub async fn mark_trade_closed(
        &self,
        id: i64,
        exit_price_a: f64,
        exit_price_b: f64,
        exit_spread: f64,
        exec_net_pnl: f64,
        exec_gross_pnl: f64,
        exec_pnl_a: f64,
        exec_pnl_b: f64,
        reason: &str,
        fund_cost: f64,
    ) {
        let r = reason.to_string();
        let current_ms = now_ms();
        let _ = self.db.call(move |conn| {
            conn.execute(
                "UPDATE trades SET status='CLOSED',exit_price_a=?,exit_price_b=?,exit_spread=?,exit_time=?,pnl=?,gross_pnl=?,pnl_a=?,pnl_b=?,curr_gross_pnl=?,close_reason=?,funding_cost=? WHERE id=?",
                params![exit_price_a, exit_price_b, exit_spread, current_ms, exec_net_pnl, exec_gross_pnl, exec_pnl_a, exec_pnl_b, exec_gross_pnl, r, fund_cost, id]
            )?;
            Ok(())
        }).await;
    }

    pub async fn update_trade_real_exit(
        &self,
        id: i64,
        real_exit_a: f64,
        real_exit_b: f64,
        real_pna: f64,
        real_pnb: f64,
        real_gp: f64,
        exit_fee: f64,
        exit_slip: f64,
    ) {
        let _ = self.db.call(move |conn| {
            conn.execute(
                "UPDATE trades SET exit_price_a=?,exit_price_b=?,pnl_a=?,pnl_b=?,gross_pnl=?,commission_cost=commission_cost+?,slippage_cost=slippage_cost+?,curr_gross_pnl=?, pnl=(?-funding_cost-(commission_cost+?)) WHERE id=?",
                params![real_exit_a, real_exit_b, real_pna, real_pnb, real_gp, exit_fee, exit_slip, real_gp, real_gp, exit_fee, id]
            )?;
            Ok(())
        }).await;
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
    ) {
        let _ = self.db.call(move |conn| {
            conn.execute(
                "INSERT INTO trades (pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,entry_spread,entry_time,highest_spread_reached,status,pnl_a,pnl_b,profile_id,commission_cost,slippage_cost,funding_cost,order_id_a,order_id_b,contracts_a,contracts_b) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params![pair, exa, exb, dir, vol, fill_a, fill_b, sp, entry_time, sp, "OPEN", 0.0f64, 0.0f64, profile_id, entry_comm, entry_slip, 0.0f64, oid_a_db, oid_b_db, contracts_a, contracts_b]
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
        let _ = self.db.call(move |conn| {
            conn.execute("UPDATE trades SET highest_spread_reached=? WHERE id=?", params![top_spread, id])?;
            Ok(())
        }).await;
    }

    /// Update open trade live state in a single batched transaction.
    /// Persists real-time prices, net PnL, real funding rates, and the accumulated
    /// funding cost (computed from live exchange API rates + hours since entry).
    pub async fn flush_live_updates(&self, live_updates: Vec<(i64, f64, f64, f64, f64, f64, f64, i64, f64)>) {
        let _ = self.db.call(move |conn| {
            let tx = conn.transaction()?;
            {
                let mut stmt = tx.prepare_cached(
                    "UPDATE trades SET \
                     last_price_a=?, last_price_b=?, last_spread=?, curr_gross_pnl=?, \
                     funding_rate_a=?, funding_rate_b=?, funding_next_ts=?, \
                     funding_cost=? \
                     WHERE id=?"
                )?;
                for (id, pa, pb, cs, pnl, fra, frb, fnt, fund_cost) in live_updates {
                    stmt.execute(params![pa, pb, cs, pnl, fra, frb, fnt, fund_cost, id])?;
                }
            }
            tx.commit()?;
            Ok(())
        }).await;
    }

    pub async fn correct_entry_prices(
        &self,
        oid_a2: String,
        oid_b2: String,
        real_a: f64,
        real_b: f64,
        total_fee: f64,
    ) {
        let _ = self.db.call(move |conn| {
            if real_a > 0.0 { let _ = conn.execute("UPDATE trades SET entry_price_a=? WHERE order_id_a=? AND status='OPEN'", params![real_a, oid_a2]); }
            if real_b > 0.0 { let _ = conn.execute("UPDATE trades SET entry_price_b=? WHERE order_id_b=? AND status='OPEN'", params![real_b, oid_b2]); }
            if total_fee > 1e-9 { let _ = conn.execute("UPDATE trades SET commission_cost=? WHERE order_id_a=? AND status='OPEN'", params![total_fee, oid_a2]); }
            Ok(())
        }).await;
    }

    /// Revert a trade back to OPEN state when a close order fails to fill.
    /// Clears close_reason so the engine can re-trigger on the next cycle.
    pub async fn revert_to_open(&self, id: i64) {
        let _ = self.db.call(move |conn| {
            conn.execute(
                "UPDATE trades SET close_reason=NULL WHERE id=? AND status='OPEN'",
                params![id]
            )?;
            Ok(())
        }).await;
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
        // Previously the SQL used column references (gross_pnl, commission_cost, funding_cost)
        // which resolve to OLD row values during UPDATE, causing wrong PnL.
        let _ = self.db.call(move |conn| {
            // First read entry commission+slippage to include them in net pnl
            let (entry_comm, entry_slip): (f64, f64) = conn.query_row(
                "SELECT COALESCE(commission_cost, 0.0), COALESCE(slippage_cost, 0.0) FROM trades WHERE id=?",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?))
            ).unwrap_or((0.0, 0.0));

            let total_comm = entry_comm + exit_fee;
            let total_slip = entry_slip + exit_slip;
            let net_pnl = gross_pnl - total_comm - fund_cost;

            conn.execute(
                "UPDATE trades SET \
                 status='CLOSED', exit_price_a=?, exit_price_b=?, exit_spread=?, exit_time=?, \
                 pnl_a=?, pnl_b=?, gross_pnl=?, curr_gross_pnl=?, close_reason=?, \
                 funding_cost=?, commission_cost=?, slippage_cost=?, \
                 pnl=? \
                 WHERE id=?",
                params![
                    exit_price_a, exit_price_b, exit_spread, current_ms,
                    pnl_a, pnl_b, gross_pnl, gross_pnl, r_clone,
                    fund_cost, total_comm, total_slip,
                    net_pnl,
                    id
                ]
            )?;
            Ok(())
        }).await;
        info!("[db] CLOSED CONFIRMED id={} reason={} gross={:.4} fee={:.4} slip={:.4} fund={:.4}",
            id, r, gross_pnl, exit_fee, exit_slip, fund_cost);
    }
}

