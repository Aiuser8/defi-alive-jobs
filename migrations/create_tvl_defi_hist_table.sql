-- Migration: Create clean.tvl_defi_hist table for historical DeFi TVL data
-- Date: 2025-09-27
-- Purpose: Store historical Total Value Locked data for DeFi ecosystem

-- Create the table
CREATE TABLE IF NOT EXISTS clean.tvl_defi_hist (
  date INTEGER NOT NULL,  -- Unix timestamp (e.g., 1609459200 for 2021-01-01)
  tvl NUMERIC NOT NULL,   -- Total Value Locked in USD (e.g., 15000000000 for $15B)
  inserted_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (date)
);

-- Add index for efficient date range queries
CREATE INDEX IF NOT EXISTS idx_tvl_defi_hist_date ON clean.tvl_defi_hist (date);

-- Add comments for documentation
COMMENT ON TABLE clean.tvl_defi_hist IS 'Historical DeFi Total Value Locked (TVL) data by date. Simple schema with unix timestamp and TVL amount.';
COMMENT ON COLUMN clean.tvl_defi_hist.date IS 'Unix timestamp representing the date (e.g., 1609459200 for 2021-01-01)';
COMMENT ON COLUMN clean.tvl_defi_hist.tvl IS 'Total Value Locked in USD for that date (e.g., 15000000000 for $15B)';
COMMENT ON COLUMN clean.tvl_defi_hist.inserted_at IS 'Timestamp when record was inserted into the database';

-- Example usage:
-- INSERT INTO clean.tvl_defi_hist (date, tvl) VALUES (1609459200, 15000000000);
-- SELECT * FROM clean.tvl_defi_hist WHERE date >= 1609459200 ORDER BY date;
