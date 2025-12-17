const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, goToQueue, captureScreensFor, teardown, doUntil, findTableRow } = require('../utils')
const { createQueue, deleteQueue, getManagementUrl, basicAuthorization } = require('../mgt-api')
const mqtt = require('mqtt')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const QueuesAndStreamsPage = require('../pageobjects/QueuesAndStreamsPage')
const QueuePage = require('../pageobjects/QueuePage')
const ConnectionsPage = require('../pageobjects/ConnectionsPage');


describe('Given a mqtt 5.0 connection with a qos 0 subscription with zero sessionExpiryInterval', function () {
  let driver
  let login
<<<<<<< HEAD:selenium/test/queuesAndStreams/view-mqtt-qos.js
  let queuesAndStreamsPage
=======
>>>>>>> d189b51a4 (Explicitily declare driver in the describe context):selenium/test/queuesAndStreams/view-mqtt-qos0.js
  let queuePage
  let overview
  let captureScreen
  let queueName
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
    queuePage = new QueuePage(driver)
    connectionsPage = new ConnectionsPage(driver)
    queuesAndStreamsPage = new QueuesAndStreamsPage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('management', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    //await overview.selectRefreshOption("Do not refresh")

    queueName = "test_" + Math.floor(Math.random() * 1000)
    createQueue(getManagementUrl(), basicAuthorization("management", "guest"), 
       "/", queueName, {
        "x-queue-type": "quorum"
      })

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

    mqttClient = mqtt.connect(mqttUrl, mqttOptions)
    let subscribed = new Promise((resolve, reject) => {
        mqttClient.on('error', function(err) {
          reject(err)
          assert.fail("Mqtt connection failed due to " + err)        
        }),
        mqttClient.on('connect', function(err) {
          mqttClient.subscribe(queueName, {qos:0}, function (err2) {
            if (!err2) {     
              resolve("ok")
            }else {
              reject(err2)
            }
          })
        })
      })    
    assert.equal("ok", await subscribed)

  })

  it('should be an mqtt connection listed', async function () {
    await overview.clickOnConnectionsTab()

    let table = await doUntil(async function() {       
      return connectionsPage.getConnectionsTable()
    }, function(table) { 
      return table.length > 0
    }, 6000)
    assert.equal(table[0][5], "MQTT 5-0")

  })

  it('should be an mqtt qos0 queue listed', async function () {
    await overview.clickOnQueuesTab()

    await doUntil(function() {      
      return queuesAndStreamsPage.getQueuesTable()
    }, function(table) { 
      return findTableRow(table, function(row) {
        return row[2] === 'rabbit_mqtt_qos0_queue'
      })
    })

  })

   it('can view mqtt qos0 queue', async function () {
    await overview.clickOnQueuesTab()

    let table = await doUntil(function() {
      return queuesAndStreamsPage.getQueuesTable()
    }, function(t) { 
      return findTableRow(t, function(row) {
        return row[2] === 'rabbit_mqtt_qos0_queue'
      })
    })
    let mqttQueueName = findTableRow(table, function(row) {
       return row[2] === 'rabbit_mqtt_qos0_queue'
    })[1]

    await goToQueue(driver, "/", mqttQueueName)
    await queuePage.isLoaded()

  })

  after(async function () {    
    await teardown(driver, this, captureScreen)
    if (mqttClient) mqttClient.end()
    deleteQueue(getManagementUrl(), basicAuthorization("management", "guest"), 
      "/", queueName)
  })
})