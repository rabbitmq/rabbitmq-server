const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, delay } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const QueuesAndStreamsPage = require('../pageobjects/QueuesAndStreamsPage')
const QueuePage = require('../pageobjects/QueuePage')
const StreamPage = require('../pageobjects/StreamPage')

describe('Streams', function () {
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
  it('add stream and view it', async function () {
     await queuesAndStreams.ensureAddQueueSectionIsVisible()
     let queueName = "test_" + Math.floor(Math.random() * 1000)
     await queuesAndStreams.fillInAddNewQueue({"name" : queueName, "type" : "stream"})
     await delay(5000)
     await queuesAndStreams.filterQueues(queueName)
     await delay(2000)
     let table = await queuesAndStreams.getQueuesTable(5)
     assert.equal(1, table.length)
     assert.equal(table[0][0], '/')
     assert.equal(table[0][1], queueName)
     assert.equal(table[0][2], 'stream')
     assert.equal(table[0][4], 'running')
 
     await queuesAndStreams.clickOnQueue("%2F", queueName)
     await stream.isLoaded()
     assert.equal(queueName, await stream.getName())
      
   })
 

  after(async function () {
    await stream.ensureDeleteQueueSectionIsVisible()
    await stream.deleteStream()

    await teardown(driver, this, captureScreen)
  })
})
