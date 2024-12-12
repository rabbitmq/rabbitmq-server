const assert = require('assert')
const { tokenFor, openIdConfiguration } = require('../utils')
const { reset, expectUser, expectVhost, expectResource, allow, verifyAll } = require('../mock_http_backend')
const { open: openAmqp, once: onceAmqp, on: onAmqp, close: closeAmqp } = require('../amqp')

var receivedAmqpMessageCount = 0
var untilConnectionEstablished = new Promise((resolve, reject) => {
  onAmqp('connection_open', function(context) {
    resolve()
  })
})

<<<<<<< HEAD

=======
onAmqp('message', function (context) {
    receivedAmqpMessageCount++
})
>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)
onceAmqp('sendable', function (context) {
    context.sender.send({body:'first message'})    
})

const profiles = process.env.PROFILES || ""
var backends = ""
for (const element of profiles.split(" ")) {
  if ( element.startsWith("auth_backends-") ) {
    backends = element.substring(element.indexOf("-")+1)
  }
}

describe('Having AMQP 1.0 protocol enabled and the following auth_backends: ' + backends, function () {
  let expectations = []
  let username = process.env.RABBITMQ_AMQP_USERNAME
  let password = process.env.RABBITMQ_AMQP_PASSWORD
  let amqp;

  before(function () {
    if (backends.includes("http") && username.includes("http")) {
      reset()
      expectations.push(expectUser({ "username": username, "password": password}, "allow"))
      expectations.push(expectVhost({ "username": username, "vhost": "/"}, "allow"))
      expectations.push(expectResource({ "username": username, "vhost": "/", "resource": "queue", "name": "my-queue", "permission":"configure", "tags":""}, "allow"))
      expectations.push(expectResource({ "username": username, "vhost": "/", "resource": "queue", "name": "my-queue", "permission":"read", "tags":""}, "allow"))
      expectations.push(expectResource({ "username": username, "vhost": "/", "resource": "exchange", "name": "amq.default", "permission":"write", "tags":""}, "allow"))
    }else if (backends.includes("oauth") && username.includes("oauth")) {
      let oauthProviderUrl = process.env.OAUTH_PROVIDER_URL
      let oauthClientId = process.env.OAUTH_CLIENT_ID
      let oauthClientSecret = process.env.OAUTH_CLIENT_SECRET
      console.log("oauthProviderUrl  : " + oauthProviderUrl)
      let openIdConfig = openIdConfiguration(oauthProviderUrl)
      console.log("Obtained token_endpoint : " + openIdConfig.token_endpoint)
      password = tokenFor(oauthClientId, oauthClientSecret, openIdConfig.token_endpoint)
      console.log("Obtained access token : " + password)
    }
  })

  it('can open an AMQP 1.0 connection', async function () {     
<<<<<<< HEAD
    var untilFirstMessageReceived = new Promise((resolve, reject) => {
      onAmqp('message', function(context) {
        resolve()
      })
    })
    amqp = openAmqp()
    await untilConnectionEstablished
    await untilFirstMessageReceived
    var untilSecondMessageReceived = new Promise((resolve, reject) => {
=======
    amqp = openAmqp()
    await untilConnectionEstablished
    var untilMessageReceived = new Promise((resolve, reject) => {
>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)
      onAmqp('message', function(context) {
        resolve()
      })
    })
    amqp.sender.send({body:'second message'})    
<<<<<<< HEAD
    await untilSecondMessageReceived    
=======
    await untilMessageReceived
    assert.equal(2, receivedAmqpMessageCount)
>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)
  })

  after(function () {
    if ( backends.includes("http") ) {
      verifyAll(expectations)
    }
    try {
      if (amqp != null) {
        closeAmqp(amqp.connection)
      }
    } catch (error) {
<<<<<<< HEAD
      console.error("Failed to close amqp10 connection due to " + error);
    }
=======
      console.error("Failed to close amqp10 connection due to " + error);      
    }  
>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)
  })
})
