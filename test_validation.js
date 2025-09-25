// test_validation.js - Test the data validation functions locally
const {
  validateTokenPrice,
  validateLendingMarket,
  validateEtfFlow,
  validateStablecoinMcap
} = require('./api/data_validation');

console.log('🧪 Testing Data Validation Functions\n');

// Test 1: Valid token price data
console.log('Test 1: Valid Token Price Data');
const validTokenPrice = {
  price: 1500.50,
  timestamp: Math.floor(Date.now() / 1000) - 300, // 5 minutes ago
  symbol: 'ETH',
  confidence: 0.95
};
const result1 = validateTokenPrice(validTokenPrice);
console.log('✅ Result:', result1);
console.log('');

// Test 2: Invalid token price data (negative price)
console.log('Test 2: Invalid Token Price Data (Negative Price)');
const invalidTokenPrice = {
  price: -100,
  timestamp: Math.floor(Date.now() / 1000) - 300,
  symbol: 'INVALID',
  confidence: 0.5
};
const result2 = validateTokenPrice(invalidTokenPrice);
console.log('❌ Result:', result2);
console.log('');

// Test 3: Outlier token price data (sudden price change)
console.log('Test 3: Outlier Token Price Data (Sudden Price Change)');
const outlierTokenPrice = {
  price: 50000, // Assuming previous price was around 1500
  timestamp: Math.floor(Date.now() / 1000) - 300,
  symbol: 'ETH',
  confidence: 0.8
};
const result3 = validateTokenPrice(outlierTokenPrice, 1500);
console.log('⚠️ Result:', result3);
console.log('');

// Test 4: Valid lending market data
console.log('Test 4: Valid Lending Market Data');
const validLendingData = {
  market_id: 'aave-v2-ethereum',
  timestamp: Math.floor(Date.now() / 1000) - 600,
  totalSupplyUsd: 1000000,
  totalBorrowUsd: 500000,
  apyBase: 3.5,
  apyReward: 1.2,
  apyBaseBorrow: 5.8,
  apyRewardBorrow: 0.5
};
const result4 = validateLendingMarket(validLendingData);
console.log('✅ Result:', result4);
console.log('');

// Test 5: Invalid lending market data (extreme APY)
console.log('Test 5: Invalid Lending Market Data (Extreme APY)');
const invalidLendingData = {
  market_id: 'aave-v2-ethereum',
  timestamp: Math.floor(Date.now() / 1000) - 600,
  totalSupplyUsd: 1000000,
  totalBorrowUsd: 500000,
  apyBase: 50000, // 50,000% APY - unrealistic
  apyReward: 1.2,
  apyBaseBorrow: 5.8,
  apyRewardBorrow: 0.5
};
const result5 = validateLendingMarket(invalidLendingData);
console.log('❌ Result:', result5);
console.log('');

// Test 6: Valid ETF flow data
console.log('Test 6: Valid ETF Flow Data');
const validEtfData = {
  gecko_id: 'bitcoin',
  day: '2024-01-15',
  total_flow_usd: 50000000
};
const result6 = validateEtfFlow(validEtfData);
console.log('✅ Result:', result6);
console.log('');

// Test 7: Invalid ETF flow data (missing gecko_id)
console.log('Test 7: Invalid ETF Flow Data (Missing gecko_id)');
const invalidEtfData = {
  day: '2024-01-15',
  total_flow_usd: 50000000
};
const result7 = validateEtfFlow(invalidEtfData);
console.log('❌ Result:', result7);
console.log('');

// Test 8: Valid stablecoin market cap data
console.log('Test 8: Valid Stablecoin Market Cap Data');
const validStablecoinData = {
  day: '2024-01-15',
  peg: 'USD',
  amount_usd: 150000000000 // $150B
};
const result8 = validateStablecoinMcap(validStablecoinData);
console.log('✅ Result:', result8);
console.log('');

// Test 9: Invalid stablecoin market cap data (unrealistic amount)
console.log('Test 9: Invalid Stablecoin Market Cap Data (Unrealistic Amount)');
const invalidStablecoinData = {
  day: '2024-01-15',
  peg: 'USD',
  amount_usd: 1e18 // $1 quintillion - unrealistic
};
const result9 = validateStablecoinMcap(invalidStablecoinData);
console.log('❌ Result:', result9);
console.log('');

console.log('🎉 All validation tests completed!');
console.log('\nSummary:');
console.log('- Valid data should have isValid: true and quality_score >= 70');
console.log('- Invalid data should have isValid: false and specific error messages');
console.log('- Outlier data should have isOutlier: true with outlier_reason');
