const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, goToQueue, captureScreensFor, teardown, doUntil, findTableRow } = require('../utils')
const { createQueue, getManagementUrl, basicAuthorization } = require('../mgt-api')
const { openConnection, getConnectionOptions } = require('../mqtt')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const QueuesAndStreamsPage = require('../pageobjects/QueuesAndStreamsPage')
const QueuePage = require('../pageobjects/QueuePage')
const ConnectionsPage = require('../pageobjects/ConnectionsPage');


describe('Given an MQTT 5.0 connection with a qos 0 subscription with zero sessionExpiryInterval', function () {
  let login
  let queuesAndStreamsPage
  let queuePage
  let overview
  let captureScreen
  let queueName

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

    mqttClient = openConnection(getConnectionOptions())
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

   it('when the connection is closed, the mqtt qos0 queue should be removed', async function () {
    
    mqttClient.end()
    
    await overview.clickOnConnectionsTab()
    await doUntil(async function() {       
      return connectionsPage.getPagingSectionHeaderText()
    }, function(header) { 
      return header === "All connections (0)"
    }, 6000)

    await overview.clickOnQueuesTab()
    await doUntil(function() {
      return queuesAndStreamsPage.getQueuesTable()
    }, function(table) { 
      return !findTableRow(table, function(row) {
        return row[2] === 'rabbit_mqtt_qos0_queue'
      })
    })

  })

  after(async function () {    
    await teardown(driver, this, captureScreen)    
  })
})
