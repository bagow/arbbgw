ALTER TABLE trading_profiles ADD COLUMN hard_stop_percent REAL DEFAULT 1.0;
ALTER TABLE trading_profiles ADD COLUMN trailing_activation_percent REAL DEFAULT 0.1;
ALTER TABLE trading_profiles ADD COLUMN daily_loss_limit_usdt REAL DEFAULT 5.0;
ALTER TABLE trading_profiles ADD COLUMN max_total_exposure_usdt REAL DEFAULT 300.0;
