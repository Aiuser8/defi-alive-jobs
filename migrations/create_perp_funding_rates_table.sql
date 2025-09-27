-- Migration: Create update.perp_funding_rates table for perpetual futures funding rate data
-- Date: 2025-09-27
-- Purpose: Store real-time perpetual futures funding rates from various marketplaces

-- Create the table
CREATE TABLE IF NOT EXISTS update.perp_funding_rates (
  perp_id UUID NOT NULL,                    -- Unique identifier for the perp contract
  timestamp TIMESTAMPTZ NOT NULL,          -- When the data was recorded
  marketplace TEXT NOT NULL,               -- Exchange/marketplace (e.g., "Binance", "Bybit")
  market TEXT NOT NULL,                    -- Market pair (e.g., "1000000BOBUSDT")
  base_asset TEXT NOT NULL,                -- Base asset (e.g., "1000000BOT")
  funding_rate NUMERIC,                    -- Current funding rate (e.g., 0.00005)
  funding_rate_previous NUMERIC,           -- Previous funding rate (e.g., 0.00045224)
  funding_time_previous BIGINT,            -- Previous funding time as unix timestamp
  open_interest NUMERIC,                   -- Open interest amount
  index_price NUMERIC,                     -- Index price of the asset
  funding_rate_7d_average NUMERIC,         -- 7-day average funding rate
  funding_rate_7d_sum NUMERIC,             -- 7-day sum of funding rates
  funding_rate_30d_average NUMERIC,        -- 30-day average funding rate
  funding_rate_30d_sum NUMERIC,            -- 30-day sum of funding rates
  inserted_at TIMESTAMPTZ DEFAULT NOW(),   -- When record was inserted
  PRIMARY KEY (perp_id, timestamp)
);

-- Add indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_perp_funding_rates_timestamp ON update.perp_funding_rates (timestamp);
CREATE INDEX IF NOT EXISTS idx_perp_funding_rates_marketplace ON update.perp_funding_rates (marketplace);
CREATE INDEX IF NOT EXISTS idx_perp_funding_rates_market ON update.perp_funding_rates (market);
CREATE INDEX IF NOT EXISTS idx_perp_funding_rates_base_asset ON update.perp_funding_rates (base_asset);

-- Add comments for documentation
COMMENT ON TABLE update.perp_funding_rates IS 'Real-time perpetual futures funding rates from various marketplaces. Updated frequently with current funding rates, open interest, and historical averages.';
COMMENT ON COLUMN update.perp_funding_rates.perp_id IS 'Unique identifier for the perpetual contract (UUID format)';
COMMENT ON COLUMN update.perp_funding_rates.timestamp IS 'Timestamp when the funding rate data was recorded';
COMMENT ON COLUMN update.perp_funding_rates.marketplace IS 'Exchange or marketplace name (e.g., Binance, Bybit, OKX)';
COMMENT ON COLUMN update.perp_funding_rates.market IS 'Trading pair/market identifier (e.g., 1000000BOBUSDT)';
COMMENT ON COLUMN update.perp_funding_rates.base_asset IS 'Base asset symbol (e.g., 1000000BOT)';
COMMENT ON COLUMN update.perp_funding_rates.funding_rate IS 'Current funding rate as decimal (e.g., 0.00005 = 0.005%)';
COMMENT ON COLUMN update.perp_funding_rates.funding_rate_previous IS 'Previous funding rate for comparison';
COMMENT ON COLUMN update.perp_funding_rates.funding_time_previous IS 'Unix timestamp of previous funding time';
COMMENT ON COLUMN update.perp_funding_rates.open_interest IS 'Total open interest in the contract';
COMMENT ON COLUMN update.perp_funding_rates.index_price IS 'Current index price of the underlying asset';
COMMENT ON COLUMN update.perp_funding_rates.funding_rate_7d_average IS '7-day rolling average of funding rates';
COMMENT ON COLUMN update.perp_funding_rates.funding_rate_7d_sum IS '7-day cumulative sum of funding rates';
COMMENT ON COLUMN update.perp_funding_rates.funding_rate_30d_average IS '30-day rolling average of funding rates';
COMMENT ON COLUMN update.perp_funding_rates.funding_rate_30d_sum IS '30-day cumulative sum of funding rates';
COMMENT ON COLUMN update.perp_funding_rates.inserted_at IS 'Timestamp when record was inserted into database';

-- Example usage:
-- INSERT INTO update.perp_funding_rates (perp_id, timestamp, marketplace, market, base_asset, funding_rate, open_interest, index_price)
-- VALUES ('61533906-1626-45fc-b3ed-9a1103705373', '2025-07-18T18:01:44.811Z', 'Binance', '1000000BOBUSDT', '1000000BOT', 0.00005, 51144394, 0.05108);

-- Query examples:
-- SELECT * FROM update.perp_funding_rates WHERE marketplace = 'Binance' ORDER BY timestamp DESC LIMIT 10;
-- SELECT market, AVG(funding_rate) FROM update.perp_funding_rates WHERE timestamp >= NOW() - INTERVAL '1 day' GROUP BY market;
