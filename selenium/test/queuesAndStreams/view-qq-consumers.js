const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, doUntil, goToQueue,delay } = require('../utils')
const { createQueue, deleteQueue, getManagementUrl, basicAuthorization } = require('../mgt-api')
const { getAmqpUrl : getAmqpUrl } = require('../amqp')
const amqplib = require('amqplib');

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const QueuesAndStreamsPage = require('../pageobjects/QueuesAndStreamsPage')
const QueuePage = require('../pageobjects/QueuePage')
const StreamPage = require('../pageobjects/StreamPage')


describe('Given a quorum queue configured with SAC', function () {
  let driver
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

  describe("given there is a consumer (without priority) attached to the queue", function () {
    let amqp091conn
    let ch1
    let ch1Consumer 
    let ch2
    let ch2Consumer 

    before(async function() {
      let amqpUrl = getAmqpUrl() + "?frameMax=0"
      amqp091conn = await amqplib.connect(amqpUrl)
      ch1 = await amqp091conn.createChannel()      
      ch1Consumer = ch1.consume(queueName, (msg) => {}, {consumerTag: "one"})      
    })

    it('it should have one consumer listed as active', async function() {
      await doUntil(async function() {
        await queuePage.refresh()
        await queuePage.isLoaded()
        return queuePage.getConsumerCount()
      }, function(count) {
        return count.localeCompare("0") == 1
      }, 5000)
      assert.equal("1", await queuePage.getConsumerCount())
      assert.equal("Consumers (1)", await queuePage.getConsumersSectionTitle())
      await queuePage.clickOnConsumerSection()
      let consumerTable = await doUntil(async function() {
        return queuePage.getConsumersTable()
      }, function(table) {
        return table[0][6].localeCompare("single active") == 0 && 
           table[0][1].localeCompare("one") == 0
      })
      assert.equal("single active", consumerTable[0][6])
      assert.equal("one", consumerTable[0][1])
      
    })

    describe("given another consumer is added with priority", function () {
      before(async function() {
        ch2 = await amqp091conn.createChannel()            
        ch2Consumer = ch2.consume(queueName, (msg) => {}, {consumerTag: "two", priority: 10})
      })

      it('the latter consumer should be listed as active and the former waiting', async function() {
              
        await doUntil(async function() {
          await queuePage.refresh()
          await queuePage.isLoaded()
          return queuePage.getConsumerCount()
        }, function(count) {
          return count.localeCompare("2") == 0
        }, 5000)
        
        assert.equal("2", await queuePage.getConsumerCount())
        assert.equal("Consumers (2)", await queuePage.getConsumersSectionTitle())
        await queuePage.clickOnConsumerSection()
        let consumerTable = await doUntil(async function() {
          return queuePage.getConsumersTable()
        }, function(table) {
          return table.length == 2 && table[0][1] != "" && table[1][1] != ""
        }, 5000)

        let activeConsumer = consumerTable[1][6].localeCompare("single active") == 0 ?
          1 : 0
        let nonActiveConsumer = activeConsumer == 1 ? 0 : 1

        assert.equal("waiting", consumerTable[nonActiveConsumer][6])
        assert.equal("one", consumerTable[nonActiveConsumer][1])
        assert.equal("single active", consumerTable[activeConsumer][6])
        assert.equal("two", consumerTable[activeConsumer][1])
        await delay(5000) 
      })
    })

    after(async function() {      
      try {
        if (amqp091conn != null) {
          amqp091conn.close()
        }
      } catch (error) {
        error("Failed to close amqp091 connection due to " + error);      
      }
      // ensure there are no more consumers 
      await doUntil(async function() {
          await queuePage.refresh()
          await queuePage.isLoaded()
          return queuePage.getConsumerCount()
        }, function(count) {
          return count.localeCompare("0") == 0
        }, 5000)
        
      
    })
  })

  describe("given there is a consumer (with priority) attached to the queue", function () {
    let amqp091conn
    let ch1
    let ch1Consumer 
    let ch2
    let ch2Consumer 

    before(async function() {
      let amqpUrl = getAmqpUrl() + "?frameMax=0"
      amqp091conn = await amqplib.connect(amqpUrl)
      ch1 = await amqp091conn.createChannel()      
      ch1Consumer = ch1.consume(queueName, (msg) => {}, {consumerTag: "one", priority: 10})      
    })

    it('it should have one consumer listed as active', async function() {
      await doUntil(async function() {
        await queuePage.refresh()
        await queuePage.isLoaded()
        return queuePage.getConsumerCount()
      }, function(count) {
        return count.localeCompare("0") == 1
      }, 5000)
      assert.equal("1", await queuePage.getConsumerCount())
      assert.equal("Consumers (1)", await queuePage.getConsumersSectionTitle())
      await queuePage.clickOnConsumerSection()
      let consumerTable = await doUntil(async function() {
        return queuePage.getConsumersTable()
      }, function(table) {
        return table[0][6].localeCompare("single active") == 0 && 
           table[0][1].localeCompare("one") == 0
      })
      assert.equal("single active", consumerTable[0][6])
      assert.equal("one", consumerTable[0][1])
      
    })

    describe("given another consumer is added without priority", function () {
      before(async function() {
        ch2 = await amqp091conn.createChannel()            
        ch2Consumer = ch2.consume(queueName, (msg) => {}, {consumerTag: "two"})
      })

      it('the former consumer should still be active and the latter be waiting', async function() {
              
        await doUntil(async function() {
          await queuePage.refresh()
          await queuePage.isLoaded()
          return queuePage.getConsumerCount()
        }, function(count) {
          return count.localeCompare("2") == 0
        }, 5000)
        
        assert.equal("2", await queuePage.getConsumerCount())
        assert.equal("Consumers (2)", await queuePage.getConsumersSectionTitle())
        await queuePage.clickOnConsumerSection()
        let consumerTable = await doUntil(async function() {
          return queuePage.getConsumersTable()
        }, function(table) {
          return table.length == 2 && table[0][1] != "" && table[1][1] != ""
        }, 5000)

        let activeConsumer = consumerTable[1][6].localeCompare("single active") == 0 ?
          1 : 0
        let nonActiveConsumer = activeConsumer == 1 ? 0 : 1

        assert.equal("waiting", consumerTable[nonActiveConsumer][6])
        assert.equal("two", consumerTable[nonActiveConsumer][1])
        assert.equal("single active", consumerTable[activeConsumer][6])
        assert.equal("one", consumerTable[activeConsumer][1])
        await delay(5000) 
      })
    })

    after(function() {      
      try {
        if (amqp091conn != null) {
          amqp091conn.close()
        }
      } catch (error) {
        error("Failed to close amqp091 connection due to " + error);      
      }
      
    })
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
    deleteQueue(getManagementUrl(), basicAuthorization("management", "guest"), 
      "/", queueName)
  })
})
