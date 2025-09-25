-- Scrub Tables Schema for Data Quality Pipeline
-- These tables store rejected data, errors, and outliers for review

-- 1. Token Price Scrub Table
CREATE SCHEMA IF NOT EXISTS scrub;

CREATE TABLE IF NOT EXISTS scrub.token_price_scrub (
  id SERIAL PRIMARY KEY,
  coin_id TEXT NOT NULL,
  symbol TEXT,
  price_usd NUMERIC,
  confidence DECIMAL(3,2),
  decimals INTEGER,
  price_timestamp TIMESTAMPTZ,
  
  -- Quality assessment
  validation_errors TEXT[],
  quality_score INTEGER, -- 0-100
  is_outlier BOOLEAN DEFAULT FALSE,
  outlier_reason TEXT,
  
  -- Original data for reference
  original_data JSONB,
  
  -- Processing metadata
  processed_at TIMESTAMPTZ DEFAULT NOW(),
  job_run_id TEXT,
  retry_count INTEGER DEFAULT 0
);

-- 2. Lending Market Scrub Table
CREATE TABLE IF NOT EXISTS scrub.lending_market_scrub (
  id SERIAL PRIMARY KEY,
  market_id TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  
  -- Lending data
  total_supply_usd NUMERIC,
  total_borrow_usd NUMERIC,
  debt_ceiling_usd NUMERIC,
  apy_base_supply NUMERIC,
  apy_reward_supply NUMERIC,
  apy_base_borrow NUMERIC,
  apy_reward_borrow NUMERIC,
  
  -- Quality assessment
  validation_errors TEXT[],
  quality_score INTEGER,
  is_outlier BOOLEAN DEFAULT FALSE,
  outlier_reason TEXT,
  
  -- Original data for reference
  original_data JSONB,
  
  -- Processing metadata
  processed_at TIMESTAMPTZ DEFAULT NOW(),
  job_run_id TEXT,
  retry_count INTEGER DEFAULT 0
);

-- 3. ETF Flow Scrub Table
CREATE TABLE IF NOT EXISTS scrub.etf_flow_scrub (
  id SERIAL PRIMARY KEY,
  gecko_id TEXT NOT NULL,
  day DATE NOT NULL,
  total_flow_usd NUMERIC,
  
  -- Quality assessment
  validation_errors TEXT[],
  quality_score INTEGER,
  is_outlier BOOLEAN DEFAULT FALSE,
  outlier_reason TEXT,
  
  -- Original data for reference
  original_data JSONB,
  
  -- Processing metadata
  processed_at TIMESTAMPTZ DEFAULT NOW(),
  job_run_id TEXT,
  retry_count INTEGER DEFAULT 0
);

-- 4. Stablecoin Market Cap Scrub Table
CREATE TABLE IF NOT EXISTS scrub.stablecoin_mcap_scrub (
  id SERIAL PRIMARY KEY,
  day DATE NOT NULL,
  peg TEXT NOT NULL,
  amount_usd NUMERIC,
  
  -- Quality assessment
  validation_errors TEXT[],
  quality_score INTEGER,
  is_outlier BOOLEAN DEFAULT FALSE,
  outlier_reason TEXT,
  
  -- Original data for reference
  original_data JSONB,
  
  -- Processing metadata
  processed_at TIMESTAMPTZ DEFAULT NOW(),
  job_run_id TEXT,
  retry_count INTEGER DEFAULT 0
);

-- 5. Data Quality Summary Table
CREATE TABLE IF NOT EXISTS scrub.data_quality_summary (
  id SERIAL PRIMARY KEY,
  job_name TEXT NOT NULL,
  job_run_id TEXT NOT NULL,
  run_timestamp TIMESTAMPTZ DEFAULT NOW(),
  
  -- Quality metrics
  total_records INTEGER,
  clean_records INTEGER,
  scrubbed_records INTEGER,
  error_records INTEGER,
  outlier_records INTEGER,
  
  -- Quality score
  overall_quality_score DECIMAL(5,2),
  
  -- Processing time
  processing_time_ms INTEGER,
  
  -- Error summary
  error_summary JSONB,
  
  UNIQUE(job_name, job_run_id)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_token_price_scrub_processed_at ON scrub.token_price_scrub(processed_at);
CREATE INDEX IF NOT EXISTS idx_token_price_scrub_quality_score ON scrub.token_price_scrub(quality_score);
CREATE INDEX IF NOT EXISTS idx_token_price_scrub_is_outlier ON scrub.token_price_scrub(is_outlier);

CREATE INDEX IF NOT EXISTS idx_lending_market_scrub_processed_at ON scrub.lending_market_scrub(processed_at);
CREATE INDEX IF NOT EXISTS idx_lending_market_scrub_quality_score ON scrub.lending_market_scrub(quality_score);
CREATE INDEX IF NOT EXISTS idx_lending_market_scrub_is_outlier ON scrub.lending_market_scrub(is_outlier);

CREATE INDEX IF NOT EXISTS idx_etf_flow_scrub_processed_at ON scrub.etf_flow_scrub(processed_at);
CREATE INDEX IF NOT EXISTS idx_etf_flow_scrub_quality_score ON scrub.etf_flow_scrub(quality_score);

CREATE INDEX IF NOT EXISTS idx_stablecoin_mcap_scrub_processed_at ON scrub.stablecoin_mcap_scrub(processed_at);
CREATE INDEX IF NOT EXISTS idx_stablecoin_mcap_scrub_quality_score ON scrub.stablecoin_mcap_scrub(quality_score);

CREATE INDEX IF NOT EXISTS idx_data_quality_summary_run_timestamp ON scrub.data_quality_summary(run_timestamp);
CREATE INDEX IF NOT EXISTS idx_data_quality_summary_job_name ON scrub.data_quality_summary(job_name);
