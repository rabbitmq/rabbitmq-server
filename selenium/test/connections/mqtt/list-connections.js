const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, doUntil } = require('../../utils')
const mqtt = require('mqtt')

const LoginPage = require('../../pageobjects/LoginPage')
const OverviewPage = require('../../pageobjects/OverviewPage')
const ConnectionsPage = require('../../pageobjects/ConnectionsPage');


describe('List MQTT connections', function () {
  let login
  let overview
  let captureScreen
  let mqttOptions

  let mqttProtocol = process.env.MQTT_PROTOCOL || 'mqtt'
  let usemtls = process.env.MQTT_USE_MTLS || false
  let rabbit = process.env.RABBITMQ_HOSTNAME || 'localhost'
  let mqttUrl = process.env.RABBITMQ_MQTT_URL || "mqtt://" + rabbit + ":1883"
  let username = process.env.RABBITMQ_AMQP_USERNAME || 'management'
  let password = process.env.RABBITMQ_AMQP_PASSWORD || 'guest'
  let client_id = process.env.RABBITMQ_AMQP_USERNAME || 'selenium-client'
  let mqttClient 

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    connectionsPage = new ConnectionsPage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('management', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    
    mqttOptions = {
      clientId: client_id,
      protocolId: 'MQTT',
      protocol: mqttProtocol,
      protocolVersion: 5,
      keepalive: 10000,
      clean: false,
      reconnectPeriod: '1000',
      properties: {
        sessionExpiryInterval: 0 
      }
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

  it('mqtt 5.0 connection', async function () {
    mqttClient = mqtt.connect(mqttUrl, mqttOptions)
    let connected = new Promise((resolve, reject) => {
      mqttClient.on('error', function(err) {
        reject(err)
        assert.fail("Mqtt connection failed due to " + err)        
      }),
      mqttClient.on('connect', function(err2) {
        resolve("ok")                
      })
    })    
    assert.equal("ok", await connected)
    
    try {
      await overview.clickOnConnectionsTab()
      
      let table = await doUntil(async function() {       
        return connectionsPage.getConnectionsTable()
      }, function(table) { 
        return table.length > 0
      }, 6000)
      assert.equal(table[0][5], "MQTT 5-0")

    } finally {
      if (mqttClient) mqttClient.end()
    }

  })

  after(async function () {    
    await teardown(driver, this, captureScreen)
  
  })
})
