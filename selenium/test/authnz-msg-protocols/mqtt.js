const fs = require('fs')
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
  let mqttProtocol = process.env.MQTT_PROTOCOL || 'mqtt'
  let usemtls = process.env.MQTT_USE_MTLS || false
  let rabbit = process.env.RABBITMQ_HOSTNAME || 'localhost'
  let mqttUrl = process.env.RABBITMQ_MQTT_URL || "mqtt://" + rabbit + ":1883"
  let username = process.env.RABBITMQ_AMQP_USERNAME
  let password = process.env.RABBITMQ_AMQP_PASSWORD
  let client_id = process.env.RABBITMQ_AMQP_USERNAME || 'selenium-client'
  
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
      protocol: mqttProtocol,
      protocolVersion: 4,
      keepalive: 10000,
      clean: false,
      reconnectPeriod: '1000'
    }
    if (mqttProtocol == 'mqtts') {
      mqttOptions["ca"] = [fs.readFileSync(process.env.RABBITMQ_CERTS + "/ca_rabbitmq_certificate.pem")]      
    } 
    if (usemtls) {
      mqttOptions["cert"] = fs.readFileSync(process.env.RABBITMQ_CERTS + "/client_rabbitmq_certificate.pem")
      mqttOptions["key"] = fs.readFileSync(process.env.RABBITMQ_CERTS + "/client_rabbitmq_key.pem")
    } else {
      mqttOptions["username"] = username
      mqttOptions["password"] = password
    }
  })

  it('can open an MQTT connection', function () {
    var client = mqtt.connect(mqttUrl, mqttOptions)
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
