const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, doWhile, goToQueue } = require('../utils')
const { createQueue, deleteQueue, getManagementUrl, basicAuthorization } = require('../mgt-api')
const { open: openAmqp, once: onceAmqp, on: onAmqp, close: closeAmqp, 
        openReceiver : openReceiver} = require('../amqp')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const QueuesAndStreamsPage = require('../pageobjects/QueuesAndStreamsPage')
const QueuePage = require('../pageobjects/QueuePage')
const StreamPage = require('../pageobjects/StreamPage')

var untilConnectionEstablished = new Promise((resolve, reject) => {
  onAmqp('connection_open', function(context) {
    console.log("Amqp connection opened")
    resolve()
  })
})

describe('Given a quorum queue configured with SAC', function () {
  let login
  let queuesAndStreams
  let queuePage
  let queueName
  let stream
  let overview
  let captureScreen
  
  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    queuesAndStreams = new QueuesAndStreamsPage(driver)
    queuePage = new QueuePage(driver)
    stream = new StreamPage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('management', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    await overview.selectRefreshOption("Do not refresh")
    await overview.clickOnQueuesTab()
    queueName = "test_" + Math.floor(Math.random() * 1000)
  
    createQueue(getManagementUrl(), basicAuthorization("management", "guest"), 
       "/", queueName, {
        "x-queue-type": "quorum", 
        "x-single-active-consumer": true
      })
    
    await goToQueue(driver, "/", queueName)
    await queuePage.isLoaded()
    assert.equal(queueName, await queuePage.getName())
    
  })

  it('it must display its queue-type and durability', async function () {    
    let table = await queuePage.getFeatures()
    assert.equal(table[0].name, "arguments:")
    let expectedArguments = [
      {"name":"x-queue-type:", "value":"quorum"}      
    ]
    assert.equal(JSON.stringify(table[0].value), JSON.stringify(expectedArguments))
    assert.equal(table[1].name, "x-single-active-consumer:")
    assert.equal(table[1].value, "true")        
    assert.equal(table[2].name, "durable:")
    assert.equal(table[2].value, "true")        
  })

  it('it should not have any consumers', async function() {
    assert.equal("0", await queuePage.getConsumerCount())
    assert.equal("Consumers (0)", await queuePage.getConsumersSectionTitle())
  })

  describe("given there is a consumer attached to the queue", function () {
    let amqp 
    before(async function() {
      amqp = openAmqp(queueName)
      await untilConnectionEstablished          
    })

    it('it should have one consumer', async function() {
      await doWhile(async function() {
        await queuePage.refresh()
        await queuePage.isLoaded()
        return queuePage.getConsumerCount()
      }, function(count) {
        return count.localeCompare("0") == 1
      }, 5000)
      assert.equal("1", await queuePage.getConsumerCount())
      assert.equal("Consumers (1)", await queuePage.getConsumersSectionTitle())
      await queuePage.clickOnConsumerSection()
      let consumerTable = await queuePage.getConsumersTable()
      
      assert.equal("single active", consumerTable[0][6])
      assert.equal("●", consumerTable[0][5])
    })

    it('it should have two consumers, after adding a second subscriber', async function() {
      openReceiver(amqp, queueName)
      await doWhile(async function() {
        await queuePage.refresh()
        await queuePage.isLoaded()
        return queuePage.getConsumerCount()
      }, function(count) {
        return count.localeCompare("2") == 0
      }, 5000)
      assert.equal("2", await queuePage.getConsumerCount())
      assert.equal("Consumers (2)", await queuePage.getConsumersSectionTitle())
      await queuePage.clickOnConsumerSection()
      let consumerTable = await queuePage.getConsumersTable()
      
      let activeConsumer = consumerTable[1][6].localeCompare("single active") == 0 ?
        1 : 0
      let nonActiveConsumer = activeConsumer == 1 ? 0 : 1

      assert.equal("waiting", consumerTable[nonActiveConsumer][6])
      assert.equal("○", consumerTable[nonActiveConsumer][5])
      assert.equal("single active", consumerTable[activeConsumer][6])
      assert.equal("●", consumerTable[activeConsumer][5])
    })

    after(function() {
      try {
        if (amqp != null) {
          closeAmqp(amqp.connection)
        }
      } catch (error) {
        error("Failed to close amqp10 connection due to " + error);      
      }  
    })
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
    deleteQueue(getManagementUrl(), basicAuthorization("management", "guest"), 
      "/", queueName)
  })
})
