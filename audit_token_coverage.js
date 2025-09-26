const fs = require('fs');
const path = require('path');

// Load the token list
const tokenList = JSON.parse(fs.readFileSync('token_list.json', 'utf-8'));

console.log(`📊 Token List Audit - Total: ${tokenList.length} tokens`);

// Calculate batch ranges for current 3-batch dispatcher
const BATCH_SIZE = 1220;
const batches = [];
for (let i = 0; i < 3; i++) {
  const start = i * BATCH_SIZE;
  const end = Math.min(start + BATCH_SIZE, tokenList.length);
  if (start < tokenList.length) {
    batches.push({
      batch: i + 1,
      start,
      end: end - 1,
      count: end - start,
      tokens: tokenList.slice(start, end)
    });
  }
}

console.log('\n🎯 Current Dispatcher Batches:');
batches.forEach(batch => {
  const chainDist = batch.tokens.reduce((acc, token) => {
    acc[token.chain] = (acc[token.chain] || 0) + 1;
    return acc;
  }, {});
  
  console.log(`\nBatch ${batch.batch}: ${batch.start}-${batch.end} (${batch.count} tokens)`);
  console.log('  Top chains:', Object.entries(chainDist)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([chain, count]) => `${chain}:${count}`)
    .join(', ')
  );
});

// Find major tokens by checking for common patterns
const majorTokenPatterns = [
  /usdc/i, /usdt/i, /weth/i, /wbtc/i, /uni/i, /link/i, /aave/i, /comp/i,
  /mkr/i, /snx/i, /dai/i, /bal/i, /crv/i, /yfi/i, /sushi/i, /1inch/i
];

console.log('\n🔍 Major Token Distribution Analysis:');
const majorTokens = tokenList.filter((token, index) => {
  const isEthereumMainnet = token.chain === 'ethereum';
  const hasLowAddress = parseInt(token.address.slice(2, 10), 16) < 0x10000000; // Likely early/major tokens
  const matchesPattern = majorTokenPatterns.some(pattern => 
    pattern.test(token.address) || pattern.test(token.chain)
  );
  
  return isEthereumMainnet && (hasLowAddress || matchesPattern);
});

console.log(`Found ${majorTokens.length} potential major tokens`);
console.log('First 10 major tokens:');
majorTokens.slice(0, 10).forEach((token, i) => {
  console.log(`  ${tokenList.indexOf(token)}: ${token.chain}:${token.address}`);
});

// Check coverage gaps
const ethereumTokens = tokenList.filter(t => t.chain === 'ethereum');
const currentlyCovered = tokenList.slice(0, BATCH_SIZE * 3);
const ethereumCovered = currentlyCovered.filter(t => t.chain === 'ethereum');

console.log(`\n📈 Coverage Analysis:`);
console.log(`  Ethereum tokens total: ${ethereumTokens.length}`);
console.log(`  Ethereum in current batches: ${ethereumCovered.length}`);
console.log(`  Ethereum coverage: ${(ethereumCovered.length / ethereumTokens.length * 100).toFixed(1)}%`);

// Suggest optimization
console.log('\n💡 Recommendations:');
console.log('1. Test current batch ranges for data availability');
console.log('2. Consider reordering tokens by likely data availability');
console.log('3. Focus on major chains: ethereum, arbitrum, base, polygon first');
console.log('4. Move obscure/testnet tokens to end of list');

