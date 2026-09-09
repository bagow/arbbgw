//! Performance metrics use reconciled results; legacy rows remain in history.
use rusqlite::Connection;
use serde_json::{json, Value};
use std::collections::HashMap;

pub fn profile_stats(conn: &Connection, profile: &str) -> rusqlite::Result<Value> {
    let tx = conn.unchecked_transaction()?;
    let result = read_profile_stats(&tx, profile)?;
    tx.commit()?;
    Ok(result)
}

fn read_profile_stats(conn: &Connection, profile: &str) -> rusqlite::Result<Value> {
    let (total, pending, legacy): (i64, i64, i64) = conn.query_row(
        "SELECT COUNT(*), COALESCE(SUM(pnl IS NULL),0),
         COALESCE(SUM(accounting_status='LEGACY_UNVERIFIED'),0)
         FROM trades WHERE status='CLOSED' AND profile_id=?",
        [profile],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    let (count, pnl, wins, losses, gain, loss, duration): (
        i64,
        Option<f64>,
        i64,
        i64,
        f64,
        f64,
        Option<f64>,
    ) = conn.query_row(
        "SELECT COUNT(*),SUM(pnl),COALESCE(SUM(pnl>0),0),COALESCE(SUM(pnl<0),0),
         COALESCE(SUM(CASE WHEN pnl>0 THEN pnl ELSE 0 END),0),
         COALESCE(SUM(CASE WHEN pnl<0 THEN -pnl ELSE 0 END),0),AVG(exit_time-entry_time)
         FROM trades WHERE status='CLOSED' AND accounting_status='VERIFIED'
         AND pnl IS NOT NULL AND profile_id=?",
        [profile],
        |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
            ))
        },
    )?;
    // Select the latest window first, then restore chronological order.
    let mut stmt = conn.prepare(
        "SELECT pnl,exchange_a,exchange_b FROM (
         SELECT id,exit_time,pnl,exchange_a,exchange_b FROM trades
         WHERE status='CLOSED' AND accounting_status='VERIFIED' AND pnl IS NOT NULL
         AND profile_id=? ORDER BY exit_time DESC,id DESC LIMIT 1000)
         ORDER BY exit_time,id",
    )?;
    let curve = stmt
        .query_map([profile], |r| {
            Ok(json!({
                "pnl":r.get::<_,f64>(0)?,"ex_a":r.get::<_,String>(1)?,"ex_b":r.get::<_,String>(2)?
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut stmt = conn.prepare(
        "SELECT pair,SUM(pnl) AS net FROM trades WHERE status='CLOSED'
         AND accounting_status='VERIFIED' AND pnl IS NOT NULL AND profile_id=?
         GROUP BY pair ORDER BY net DESC,pair LIMIT 10",
    )?;
    let tokens = stmt
        .query_map([profile], |r| {
            Ok(json!({
                "token":r.get::<_,String>(0)?,"pnl":r.get::<_,f64>(1)?
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut stmt = conn.prepare(
        "SELECT exchange_a,exchange_b,SUM(pnl) FROM trades WHERE status='CLOSED'
         AND accounting_status='VERIFIED' AND pnl IS NOT NULL AND profile_id=?
         GROUP BY exchange_a,exchange_b",
    )?;
    let mut pairs: HashMap<String, f64> = HashMap::new();
    for row in stmt.query_map([profile], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, f64>(2)?,
        ))
    })? {
        let (a, b, net) = row?;
        let mut names = [a, b];
        names.sort();
        *pairs.entry(names.join("-")).or_default() += net;
    }
    let mut pairs: Vec<_> = pairs.into_iter().collect();
    pairs.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    pairs.truncate(10);
    let pairs: Vec<_> = pairs
        .into_iter()
        .map(|(pair, pnl)| json!({"pair":pair,"pnl":pnl}))
        .collect();
    let mut stmt = conn.prepare(
        "SELECT exchange_a,net_pnl_a FROM trades WHERE status='CLOSED'
         AND accounting_status='VERIFIED' AND pnl IS NOT NULL AND profile_id=?1
         UNION ALL SELECT exchange_b,net_pnl_b FROM trades WHERE status='CLOSED'
         AND accounting_status='VERIFIED' AND pnl IS NOT NULL AND profile_id=?1",
    )?;
    let mut exchanges: HashMap<String, f64> = HashMap::new();
    for row in stmt.query_map([profile], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, Option<f64>>(1)?))
    })? {
        let (exchange, net) = row?;
        if let Some(net) = net {
            *exchanges.entry(exchange).or_default() += net;
        }
    }
    Ok(json!({
        "total_trades":total,"total_pnl":pnl,"pending_count":pending,"legacy_count":legacy,
        "verified_count":count,"unverified_count":total-count,
        "wins":wins,"losses":losses,"breakeven":count-wins-losses,
        "avg_pnl":pnl.map(|p|p/count as f64),
        "win_rate_percent":if count>0 {Some(wins as f64/count as f64*100.0)} else {None},
        "profit_factor":if loss>0.0 {Some(gain/loss)} else {None},
        "avg_duration":duration.map(|d|d as i64),
        "equity_curve":curve,"pnl_by_exchange":exchanges,
        "top_tokens":tokens,"top_exchange_pairs":pairs
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn database() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(
            "CREATE TABLE trades(id INTEGER PRIMARY KEY,status TEXT,accounting_status TEXT,
          pnl REAL,entry_time INTEGER DEFAULT 0,exit_time INTEGER,profile_id TEXT DEFAULT 'p',
          pair TEXT DEFAULT 'BTCUSDT',exchange_a TEXT DEFAULT 'a',exchange_b TEXT DEFAULT 'b',
          net_pnl_a REAL,net_pnl_b REAL);",
        )
        .unwrap();
        c
    }
    #[test]
    fn legacy_and_pending_results_cannot_inflate_performance() {
        let c = database();
        c.execute_batch(
            "INSERT INTO trades(id,status,accounting_status,pnl,exit_time) VALUES
          (1,'CLOSED','VERIFIED',1,1),(2,'CLOSED','VERIFIED',1,2),
          (3,'CLOSED','VERIFIED',-10,3),(4,'CLOSED','VERIFIED',0,4),
          (5,'CLOSED','LEGACY_UNVERIFIED',1000,5),(6,'CLOSED','FEES_PENDING',NULL,6),
          (7,'OPEN','VERIFIED',999,7),(8,'CANCELLED','VERIFIED',100,8);",
        )
        .unwrap();
        let v = profile_stats(&c, "p").unwrap();
        assert_eq!(v["total_trades"], 6);
        assert_eq!(v["verified_count"], 4);
        assert_eq!(v["unverified_count"], 2);
        assert_eq!(v["wins"], 2);
        assert_eq!(v["losses"], 1);
        assert_eq!(v["breakeven"], 1);
        assert_eq!(v["total_pnl"], -8.0);
        assert_eq!(v["avg_pnl"], -2.0);
        assert_eq!(v["profit_factor"], 0.2);
        assert_eq!(v["equity_curve"].as_array().unwrap().len(), 4);
        assert_eq!(v["top_tokens"][0]["pnl"], -8.0);
        assert_eq!(v["top_exchange_pairs"][0]["pnl"], -8.0);
        assert!(profile_stats(&c, "other").unwrap()["total_pnl"].is_null());
    }
    #[test]
    fn curve_uses_latest_thousand_in_chronological_order() {
        let c = database();
        for id in 1..=1002 {
            c.execute(
                "INSERT INTO trades(id,status,accounting_status,pnl,exit_time)
              VALUES(?1,'CLOSED','VERIFIED',?1,?1)",
                [id],
            )
            .unwrap();
        }
        let v = profile_stats(&c, "p").unwrap();
        let curve = v["equity_curve"].as_array().unwrap();
        assert_eq!(curve.len(), 1000);
        assert_eq!(curve[0]["pnl"], 3.0);
        assert_eq!(curve[999]["pnl"], 1002.0);
    }
}
