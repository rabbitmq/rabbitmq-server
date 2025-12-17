const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, delay } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const QueuesAndStreamsPage = require('../pageobjects/QueuesAndStreamsPage')
const QueuePage = require('../pageobjects/QueuePage')
const StreamPage = require('../pageobjects/StreamPage')

describe('Quorum queues', function () {
  let driver
  let login
  let queuesAndStreams
  let queuePage
  let overview
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    queuesAndStreams = new QueuesAndStreamsPage(driver)
    queuePage = new QueuePage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('management', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    await overview.selectRefreshOption("Do not refresh")
    await overview.clickOnQueuesTab()
    
  })
  it('add quorum queue and view it', async function () {
    await queuesAndStreams.ensureAddQueueSectionIsVisible()
    let queueName = "test_" + Math.floor(Math.random() * 1000)
    await queuesAndStreams.fillInAddNewQueue({"name" : queueName, "type" : "quorum"})
    await delay(5000)
    await queuesAndStreams.filterQueues(queueName)
    await delay(2000)
    let table = await queuesAndStreams.getQueuesTable(5)
    assert.equal(1, table.length)
    assert.equal(table[0][0], '/')
    assert.equal(table[0][1], queueName)
    assert.equal(table[0][2], 'quorum')
    assert.equal(table[0][4], 'running')

    await queuesAndStreams.clickOnQueue("%2F", queueName)
    await queuePage.isLoaded()
    assert.equal(queueName, await queuePage.getName())
     
  })

  after(async function () {
    await queuePage.ensureDeleteQueueSectionIsVisible()
    await queuePage.deleteQueue()

    await teardown(driver, this, captureScreen)
  })
})
