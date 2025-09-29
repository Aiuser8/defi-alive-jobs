// Test endpoint to verify deployment is working
module.exports = async (req, res) => {
  const timestamp = new Date().toISOString();
  console.log(`🧪 TEST ENDPOINT CALLED AT: ${timestamp}`);
  
  res.status(200).json({
    success: true,
    message: "Test endpoint is working",
    timestamp: timestamp,
    deploymentId: "FRESH_DEPLOYMENT_" + Date.now()
  });
};
