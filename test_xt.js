const WebSocket = require('ws');
const ws = new WebSocket('wss://fstream.xt.com/ws/market');
ws.on('open', () => {
  ws.send(JSON.stringify({'method':'subscribe','params':['btc_usdt@bookTicker'],'id':1}));
});
ws.on('message', (data) => {
  console.log(data.toString());
  process.exit(0);
});
