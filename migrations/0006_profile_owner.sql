-- Migration: Add owner column for user isolation

ALTER TABLE trading_profiles ADD COLUMN owner TEXT DEFAULT 'dima';
ALTER TABLE trades ADD COLUMN owner TEXT DEFAULT 'dima';
