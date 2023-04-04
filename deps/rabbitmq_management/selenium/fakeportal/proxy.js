var http = require('http'),
    httpProxy = require('http-proxy');
const XMLHttpRequest = require('xmlhttprequest').XMLHttpRequest

const rabbitmq_url = process.env.RABBITMQ_URL || 'http://0.0.0.0:15672/';
const client_id = process.env.CLIENT_ID;
const client_secret = process.env.CLIENT_SECRET;
const uaa_url = process.env.UAA_URL;
const port = process.env.PORT;

//
// Create a proxy server with custom application logic
//
var proxy = httpProxy.createProxyServer({});

proxy.on('proxyReq', function(proxyReq, req, res, options) {
  console.log("proxing " + req.url)
  if (req.url.endsWith("bootstrap.js")) {
    proxyReq.setHeader('Authorization', 'Bearer ' + access_token(client_id, client_secret));
  }
  proxyReq.setHeader('origin', req.url)
  proxyReq.setHeader('Access-Control-Allow-Origin', '*');
  proxyReq.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');

});
var server = http.createServer(function(req, res) {
  // You can define here your custom logic to handle the request
  // and then proxy the request.
  proxy.web(req, res, {
    target: rabbitmq_url
  });
});
console.log("fakeproxy listening on port " + port + ".  RABBITMQ_URL=" + rabbitmq_url)
server.listen(port);


function default_if_blank(value, defaultValue) {
  if (typeof value === "undefined" || value === null || value == "") {
    return defaultValue;
  } else {
    return value;
  }
}

function access_token(id, secret) {
  const req = new XMLHttpRequest();
  const url = uaa_url + '/oauth/token';
  const params = 'client_id=' + id +
    '&client_secret=' + secret +
    '&grant_type=client_credentials' +
    '&token_format=jwt' +
    '&response_type=token';

  console.debug("Sending " + url + " with params "+  params);

  req.open('POST', url, false);
  req.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');
  req.setRequestHeader('Accept', 'application/json');
  req.send(params);
  console.log("Ret " + req.status)
  if (req.status == 200) {
    const token = JSON.parse(req.responseText).access_token;
    console.log("Token => " + token)
    return token;
  } else {
    throw new Error(req.status + " : " + req.responseText);
  }
}
