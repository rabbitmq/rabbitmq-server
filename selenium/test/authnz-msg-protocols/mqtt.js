const assert = require('assert')
const { tokenFor, openIdConfiguration } = require('../utils')
const { reset, expectUser, expectVhost, expectResource, allow, verifyAll } = require('../mock_http_backend')
const mqtt = require('mqtt');

const profiles = process.env.PROFILES || ""
var backends = ""
for (const element of profiles.split(" ")) {
  if ( element.startsWith("auth_backends-") ) {
    backends = element.substring(element.indexOf("-")+1)
  }
}

describe('Having MQTT protocol enbled and the following auth_backends: ' + backends, function () {
  let mqttOptions
  let expectations = []
  let client_id = 'selenium-client'
  let rabbit = process.env.RABBITMQ_HOSTNAME || 'localhost'
  let username = process.env.RABBITMQ_AMQP_USERNAME
  let password = process.env.RABBITMQ_AMQP_PASSWORD

  before(function () {
    if (backends.includes("http") && username.includes("http")) {
      reset()
      expectations.push(expectUser({ "username": username, "password": password, "client_id": client_id, "vhost": "/" }, "allow"))
      expectations.push(expectVhost({ "username": username, "vhost": "/"}, "allow"))
    } else if (backends.includes("oauth") && username.includes("oauth")) {
      let oauthProviderUrl = process.env.OAUTH_PROVIDER_URL
      let oauthClientId = process.env.OAUTH_CLIENT_ID
      let oauthClientSecret = process.env.OAUTH_CLIENT_SECRET
      let openIdConfig = openIdConfiguration(oauthProviderUrl)
      console.log("Obtained token_endpoint : " + openIdConfig.token_endpoint)
      password = tokenFor(oauthClientId, oauthClientSecret, openIdConfig.token_endpoint)
      console.log("Obtained access token : " + password)
    }
    mqttOptions = {
      clientId: client_id,
      protocolId: 'MQTT',
      protocolVersion: 4,
      keepalive: 10000,
      clean: false,
      reconnectPeriod: '1000',
      username: username,
      password: password,
    }
  })

  it('can open an MQTT connection', function () {
    var client = mqtt.connect("mqtt://" + rabbit + ":1883", mqttOptions)
    client.on('error', function(err) {
      assert.fail("Mqtt connection failed due to " + err)
      client.end()
    })
    client.on('connect', function(err) {
      client.end()
    })
  })

  after(function () {
      if ( backends.includes("http") ) {
        verifyAll(expectations)
      }
  })
})
