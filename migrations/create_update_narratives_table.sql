-- Create update.narratives table for daily FDV sector performance updates
CREATE TABLE IF NOT EXISTS update.narratives (
  date INTEGER NOT NULL,
  analytics NUMERIC,
  artificial_intelligence NUMERIC,
  bitcoin NUMERIC,
  bridge_governance_tokens NUMERIC,
  centralized_exchange_token NUMERIC,
  data_availability NUMERIC,
  decentralized_finance NUMERIC,
  decentralized_identifier NUMERIC,
  depin NUMERIC,
  ethereum NUMERIC,
  gaming_gamefi NUMERIC,
  liquid_staking_governance_tokens NUMERIC,
  meme NUMERIC,
  nft_marketplace NUMERIC,
  oracle NUMERIC,
  politifi NUMERIC,
  prediction_markets NUMERIC,
  real_world_assets NUMERIC,
  rollup NUMERIC,
  smart_contract_platform NUMERIC,
  socialfi NUMERIC,
  solana NUMERIC,
  null_category NUMERIC,
  inserted_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (date)
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_update_narratives_date ON update.narratives (date DESC);
CREATE INDEX IF NOT EXISTS idx_update_narratives_inserted_at ON update.narratives (inserted_at DESC);

-- Sector performance indexes for real-time analysis
CREATE INDEX IF NOT EXISTS idx_update_narratives_defi ON update.narratives (decentralized_finance);
CREATE INDEX IF NOT EXISTS idx_update_narratives_ai ON update.narratives (artificial_intelligence);
CREATE INDEX IF NOT EXISTS idx_update_narratives_meme ON update.narratives (meme);
CREATE INDEX IF NOT EXISTS idx_update_narratives_rwa ON update.narratives (real_world_assets);

-- Add table and column comments
COMMENT ON TABLE update.narratives IS 'Daily FDV (Fully Diluted Valuation) performance updates by crypto sector/narrative from DeFiLlama Pro API. Contains latest daily percentage performance for 20+ crypto categories.';
COMMENT ON COLUMN update.narratives.date IS 'Unix timestamp for the date (e.g., 1727395200 for Sept 27, 2024)';
COMMENT ON COLUMN update.narratives.analytics IS 'FDV performance percentage for Analytics sector tokens';
COMMENT ON COLUMN update.narratives.artificial_intelligence IS 'FDV performance percentage for AI sector tokens';
COMMENT ON COLUMN update.narratives.bitcoin IS 'FDV performance percentage for Bitcoin ecosystem tokens';
COMMENT ON COLUMN update.narratives.bridge_governance_tokens IS 'FDV performance percentage for Bridge governance tokens';
COMMENT ON COLUMN update.narratives.centralized_exchange_token IS 'FDV performance percentage for CEX tokens';
COMMENT ON COLUMN update.narratives.data_availability IS 'FDV performance percentage for Data Availability tokens';
COMMENT ON COLUMN update.narratives.decentralized_finance IS 'FDV performance percentage for DeFi tokens';
COMMENT ON COLUMN update.narratives.decentralized_identifier IS 'FDV performance percentage for DID tokens';
COMMENT ON COLUMN update.narratives.depin IS 'FDV performance percentage for DePIN tokens';
COMMENT ON COLUMN update.narratives.ethereum IS 'FDV performance percentage for Ethereum ecosystem tokens';
COMMENT ON COLUMN update.narratives.gaming_gamefi IS 'FDV performance percentage for Gaming/GameFi tokens';
COMMENT ON COLUMN update.narratives.liquid_staking_governance_tokens IS 'FDV performance percentage for Liquid Staking governance tokens';
COMMENT ON COLUMN update.narratives.meme IS 'FDV performance percentage for Meme tokens';
COMMENT ON COLUMN update.narratives.nft_marketplace IS 'FDV performance percentage for NFT Marketplace tokens';
COMMENT ON COLUMN update.narratives.oracle IS 'FDV performance percentage for Oracle tokens';
COMMENT ON COLUMN update.narratives.politifi IS 'FDV performance percentage for PolitiFi tokens';
COMMENT ON COLUMN update.narratives.prediction_markets IS 'FDV performance percentage for Prediction Markets tokens';
COMMENT ON COLUMN update.narratives.real_world_assets IS 'FDV performance percentage for RWA tokens';
COMMENT ON COLUMN update.narratives.rollup IS 'FDV performance percentage for Rollup tokens';
COMMENT ON COLUMN update.narratives.smart_contract_platform IS 'FDV performance percentage for Smart Contract Platform tokens';
COMMENT ON COLUMN update.narratives.socialfi IS 'FDV performance percentage for SocialFi tokens';
COMMENT ON COLUMN update.narratives.solana IS 'FDV performance percentage for Solana ecosystem tokens';
COMMENT ON COLUMN update.narratives.null_category IS 'FDV performance percentage for uncategorized tokens';
COMMENT ON COLUMN update.narratives.inserted_at IS 'Timestamp when record was inserted into the database';
