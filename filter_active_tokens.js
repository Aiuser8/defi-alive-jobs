const fs = require('fs');

// Configuration
const API_KEY = process.env.DEFILLAMA_API_KEY;
const BATCH_SIZE = 50; // Test in smaller batches to avoid API limits
const MAX_CONCURRENT = 3;
const FRESH_THRESHOLD_HOURS = 24; // Consider data fresh if < 24 hours old

if (!API_KEY) {
  console.error('❌ Missing DEFILLAMA_API_KEY environment variable');
  process.exit(1);
}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function testTokenBatch(tokenIds) {
  const url = `https://pro-api.llama.fi/${API_KEY}/coins/chart/${tokenIds.join(',')}`;
  
  try {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`API Error: ${response.status}`);
    }
    
    const data = await response.json();
    const results = [];
    
    for (const tokenId of tokenIds) {
      const tokenData = data.coins[tokenId];
      const hasData = tokenData && tokenData.prices && tokenData.prices.length > 0;
      
      if (hasData) {
        const latestPrice = tokenData.prices[tokenData.prices.length - 1];
        const ageHours = (Date.now() / 1000 - latestPrice.timestamp) / 3600;
        const isFresh = ageHours < FRESH_THRESHOLD_HOURS;
        
        results.push({
          tokenId,
          hasData: true,
          isFresh,
          ageHours: Math.round(ageHours * 10) / 10,
          latestTimestamp: latestPrice.timestamp,
          price: latestPrice.price
        });
      } else {
        results.push({
          tokenId,
          hasData: false,
          isFresh: false,
          ageHours: null,
          latestTimestamp: null,
          price: null
        });
      }
    }
    
    return results;
  } catch (error) {
    console.error(`❌ Error testing batch: ${error.message}`);
    return tokenIds.map(id => ({
      tokenId: id,
      hasData: false,
      isFresh: false,
      error: error.message
    }));
  }
}

async function filterActiveTokens() {
  console.log('🔍 Loading token list...');
  const tokenList = JSON.parse(fs.readFileSync('token_list.json', 'utf-8'));
  
  // Convert to coin IDs
  const coinIds = tokenList.map(token => `${token.chain}:${token.address}`);
  console.log(`📊 Total tokens to test: ${coinIds.length}`);
  
  // Create batches
  const batches = [];
  for (let i = 0; i < coinIds.length; i += BATCH_SIZE) {
    batches.push(coinIds.slice(i, i + BATCH_SIZE));
  }
  
  console.log(`📦 Created ${batches.length} batches of ${BATCH_SIZE} tokens each`);
  console.log(`⏱️  Estimated time: ${Math.ceil(batches.length / MAX_CONCURRENT * 2)} minutes`);
  
  const activeTokens = [];
  const allResults = [];
  let processedBatches = 0;
  
  // Process batches with concurrency control
  for (let i = 0; i < batches.length; i += MAX_CONCURRENT) {
    const batchGroup = batches.slice(i, i + MAX_CONCURRENT);
    
    const promises = batchGroup.map(async (batch, index) => {
      const results = await testTokenBatch(batch);
      return results;
    });
    
    const groupResults = await Promise.all(promises);
    
    // Flatten and process results
    for (const batchResults of groupResults) {
      for (const result of batchResults) {
        allResults.push(result);
        if (result.isFresh) {
          activeTokens.push(result.tokenId);
        }
      }
    }
    
    processedBatches += batchGroup.length;
    const freshCount = activeTokens.length;
    const coverage = (freshCount / allResults.length * 100).toFixed(1);
    
    console.log(`✅ Processed ${processedBatches}/${batches.length} batches | Fresh tokens: ${freshCount} (${coverage}% coverage)`);
    
    // Rate limiting
    if (i + MAX_CONCURRENT < batches.length) {
      await sleep(2000); // 2 second delay between batch groups
    }
  }
  
  // Generate summary
  const summary = {
    total_tested: allResults.length,
    tokens_with_data: allResults.filter(r => r.hasData).length,
    fresh_tokens: activeTokens.length,
    coverage_rate: (activeTokens.length / allResults.length * 100).toFixed(1),
    avg_age_hours: allResults.filter(r => r.hasData && r.ageHours !== null)
      .reduce((sum, r) => sum + r.ageHours, 0) / allResults.filter(r => r.hasData).length
  };
  
  console.log('\n📊 FILTERING COMPLETE!');
  console.log(`   Total tested: ${summary.total_tested}`);
  console.log(`   Have data: ${summary.tokens_with_data}`);
  console.log(`   Fresh (< ${FRESH_THRESHOLD_HOURS}h): ${summary.fresh_tokens}`);
  console.log(`   Coverage rate: ${summary.coverage_rate}%`);
  console.log(`   Avg age: ${summary.avg_age_hours.toFixed(1)} hours`);
  
  // Save results
  const outputData = {
    metadata: {
      filtered_at: new Date().toISOString(),
      fresh_threshold_hours: FRESH_THRESHOLD_HOURS,
      summary
    },
    active_tokens: activeTokens,
    all_results: allResults
  };
  
  fs.writeFileSync('filtered_active_tokens.json', JSON.stringify(outputData, null, 2));
  console.log('\n💾 Results saved to filtered_active_tokens.json');
  
  // Create the new filtered token list
  const filteredTokenList = tokenList.filter(token => {
    const coinId = `${token.chain}:${token.address}`;
    return activeTokens.includes(coinId);
  });
  
  fs.writeFileSync('token_list_active.json', JSON.stringify(filteredTokenList, null, 2));
  console.log(`💾 Active token list saved to token_list_active.json (${filteredTokenList.length} tokens)`);
  
  return {
    summary,
    activeTokens,
    filteredTokenList
  };
}

// Run the filter
filterActiveTokens().catch(console.error);
