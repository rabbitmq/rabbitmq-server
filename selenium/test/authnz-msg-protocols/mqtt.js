const fs = require('fs')
const assert = require('assert')
const { tokenFor, openIdConfiguration, log } = require('../utils')
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
    if (backends.includes("http") && (username.includes("http") || usemtls)) {
      reset()
      if (!usemtls) {
        expectations.push(expectUser({ 
          "username": username, 
          "password": password, 
          "client_id": client_id, 
          "vhost": "/" }, "allow"))
      } else {
        expectations.push(expectUser({ 
          "username": username, 
          "client_id": client_id, 
          "vhost": "/" }, "allow"))
      }
      expectations.push(expectVhost({ "username": username, "vhost": "/"}, "allow"))

    } else if (backends.includes("oauth") && username.includes("oauth")) {
      let oauthProviderUrl = process.env.OAUTH_PROVIDER_URL
      let oauthClientId = process.env.OAUTH_CLIENT_ID
      let oauthClientSecret = process.env.OAUTH_CLIENT_SECRET
      let scope = process.env.OAUTH_SCOPES
      let openIdConfig = openIdConfiguration(oauthProviderUrl)
      log("Obtained token_endpoint : " + openIdConfig.token_endpoint)
      password = tokenFor(oauthClientId, oauthClientSecret, openIdConfig.token_endpoint, scope)
      log("Obtained access token : " + password)
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

  it('can open an MQTT connection', async function () {
    var client = mqtt.connect(mqttUrl, mqttOptions)
    let done = new Promise((resolve, reject) => {
      client.on('error', function(err) {
        reject(err)
        client.end()
        assert.fail("Mqtt connection failed due to " + err)        
      }),
      client.on('connect', function(err) {
        resolve("ok")
        client.end()
      })
    })
    assert.equal("ok", await done)
  })

  after(function () {
      if ( backends.includes("http") ) {
        verifyAll(expectations)
      }
  })
})
