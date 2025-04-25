const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, delay } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const QueuesAndStreamsPage = require('../pageobjects/QueuesAndStreamsPage')
const QueuePage = require('../pageobjects/QueuePage')
const StreamPage = require('../pageobjects/StreamPage')

describe('Classic queues', function () {
  let login
  let queuesAndStreams
  let queue
  let stream
  let overview
  let captureScreen
  let queueName

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
    
    queueName = "test_" + Math.floor(Math.random() * 1000)
  })

  it('add classic queue and view it', async function () {
    await queuesAndStreams.ensureAddQueueSectionIsVisible()
    
    await queuesAndStreams.fillInAddNewQueue({"name" : queueName, "type" : "classic"})
    await delay(5000)
    await queuesAndStreams.filterQueues(queueName)
    await delay(2000)
    let table = await queuesAndStreams.getQueuesTable(5)
    assert.equal(1, table.length)
    assert.equal(table[0][0], '/')
    assert.equal(table[0][1], queueName)
    assert.equal(table[0][2], 'classic')
    assert.equal(table[0][4], 'running')

    await queuesAndStreams.clickOnQueue("%2F", queueName)
    await queue.isLoaded()
    assert.equal(queueName, await queue.getName())

  })

  after(async function () {
    await queue.ensureDeleteQueueSectionIsVisible()
    await queue.deleteQueue()

    await teardown(driver, this, captureScreen)
  })
})
