const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, doWhile } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const QueuesAndStreamsPage = require('../pageobjects/QueuesAndStreamsPage')
const QueuePage = require('../pageobjects/QueuePage')
const StreamPage = require('../pageobjects/StreamPage')

describe('Queues and Streams management', function () {
  let login
  let queuesAndStreams
  let queue
  let stream
  let overview
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    queuesAndStreams = new QueuesAndStreamsPage(driver)
    queue = new QueuePage(driver)
    stream = new StreamPage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('management', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    await overview.selectRefreshOption("Do not refresh")
    await overview.clickOnQueuesTab()
    
  })

  it('display summary of queues and streams', async function () {
    let text = await queuesAndStreams.getPagingSectionHeaderText()
    assert.equal(true, text.startsWith('All queues') )
  })

  it('queue selectable columns', async function () {  
    await overview.clickOnOverviewTab()
    await overview.clickOnQueuesTab()
    await queuesAndStreams.ensureAddQueueSectionIsVisible()
    let queueName = "test_" + Math.floor(Math.random() * 1000)
    await queuesAndStreams.fillInAddNewQueue({"name" : queueName, "type" : "classic"})

    await doWhile(async function() { return queuesAndStreams.getQueuesTable() },
      function(table) { 
        return table.length > 0
      })

    await queuesAndStreams.clickOnSelectTableColumns()
    let table = await queuesAndStreams.getSelectableTableColumns()
    
    assert.equal(4, table.length)
    let overviewGroup = { 
        "name" : "Overview:",
        "columns": [
          {"name:":"Type","id":"checkbox-queues-type"},
          {"name:":"Features (with policy)","id":"checkbox-queues-features"},
          {"name:":"Features (no policy)","id":"checkbox-queues-features_no_policy"},
          {"name:":"Policy","id":"checkbox-queues-policy"},
          {"name:":"Consumer count","id":"checkbox-queues-consumers"},
          {"name:":"Consumer capacity","id":"checkbox-queues-consumer_capacity"},
          {"name:":"State","id":"checkbox-queues-state"}
        ]
    }    
    assert.equal(JSON.stringify(table[0]), JSON.stringify(overviewGroup))
    let messagesGroup = { 
      "name" : "Messages:",
      "columns": [
        {"name:":"Ready","id":"checkbox-queues-msgs-ready"},
        {"name:":"Unacknowledged","id":"checkbox-queues-msgs-unacked"},
        {"name:":"In memory","id":"checkbox-queues-msgs-ram"},
        {"name:":"Persistent","id":"checkbox-queues-msgs-persistent"},
        {"name:":"Total","id":"checkbox-queues-msgs-total"}
      ]
    }    
    assert.equal(JSON.stringify(table[1]), JSON.stringify(messagesGroup))    
    let messageBytesGroup = { 
      "name" : "Message bytes:",
      "columns": [
        {"name:":"Ready","id":"checkbox-queues-msg-bytes-ready"},
        {"name:":"Unacknowledged","id":"checkbox-queues-msg-bytes-unacked"},
        {"name:":"In memory","id":"checkbox-queues-msg-bytes-ram"},
        {"name:":"Persistent","id":"checkbox-queues-msg-bytes-persistent"},
        {"name:":"Total","id":"checkbox-queues-msg-bytes-total"}
      ]
    }
    assert.equal(JSON.stringify(table[2]), JSON.stringify(messageBytesGroup))
    let messageRatesGroup = { 
      "name" : "Message rates:",
      "columns": [
        {"name:":"incoming","id":"checkbox-queues-rate-incoming"},
        {"name:":"deliver / get","id":"checkbox-queues-rate-deliver"},
        {"name:":"redelivered","id":"checkbox-queues-rate-redeliver"},
        {"name:":"ack","id":"checkbox-queues-rate-ack"}
      ]
    }
    assert.equal(JSON.stringify(table[3]), JSON.stringify(messageRatesGroup))

  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
