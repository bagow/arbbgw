-- Add pending_commands to trading_settings
ALTER TABLE trading_settings ADD COLUMN pending_commands TEXT DEFAULT NULL;
