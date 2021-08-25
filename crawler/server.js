const express = require('express');
require('express-async-errors');
const checker = require('ikea-availability-checker');


const app = express();

app.get('/crawl', async (req, res) => {
  const buCodes = JSON.parse(req.query.buCodes || '[]');
  const productId = req.query.productId.toString();
  console.log(buCodes, productId)
  const results = await Promise.all(buCodes.map(async (buCode) => {
    return await checker.availability(buCode.toString(), productId);
  }));
  res.status(200).send(results);
});
