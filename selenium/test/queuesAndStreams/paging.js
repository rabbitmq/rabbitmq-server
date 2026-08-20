const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, captureScreenshotIfFailed, teardown, doUntil } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const QueuesAndStreamsPage = require('../pageobjects/QueuesAndStreamsPage')
const QueuePage = require('../pageobjects/QueuePage')

describe('Queues Paging', function () {
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

  it('can navigate through paginated queues', async function () {
    await queuesAndStreams.ensureAddQueueSectionIsVisible()

    // Create 2 queues to test pagination with page size = 1
    const prefix = "page_test_" + Math.floor(Math.random() * 1000) + "_"
    const queue1 = prefix + "1"
    const queue2 = prefix + "2"

    await queuesAndStreams.fillInAddNewQueue({"name" : queue1, "type" : "classic"})
    assert.ok(await queuesAndStreams.isQueueVisible("%2F", queue1))
    await queuesAndStreams.fillInAddNewQueue({"name" : queue2, "type" : "classic"})
    assert.ok(await queuesAndStreams.isQueueVisible("%2F", queue2))

    // We don't use filtering here, just verify that paging works by checking
    // that page 1 and page 2 show different items when page size is 1.

    // Change page size to 1
    await queuesAndStreams.setPageSize(1)
    let table = await doUntil(async function () {
      return queuesAndStreams.getQueuesTable(2)
    }, function (table) {
      return table.length === 1
    }, 1000)

    // Verify only 1 queue is visible now
    assert.equal(1, table.length)
    const firstQueueName = table[0][1]

    // Navigate to page 2
    await queuesAndStreams.selectPage(2)
    table = await doUntil(async function () {
      return queuesAndStreams.getQueuesTable(2)
    }, function (table) {
      return table.length === 1 && table[0][1] !== firstQueueName
    }, 1000)

    // Verify a different queue is visible
    assert.equal(1, table.length)
    const secondQueueName = table[0][1]
    assert.notEqual(firstQueueName, secondQueueName)

    // Clean up
    // Set page size to a large number so we can find and click our test queues
    await queuesAndStreams.setPageSize(100)
    await doUntil(async function () {
      return queuesAndStreams.getQueuesTable(2)
    }, function (table) {
      return table.length >= 2
    }, 1000)

    await queuesAndStreams.clickOnQueue("%2F", queue1)
    await queuePage.isLoaded()
    await queuePage.ensureDeleteQueueSectionIsVisible()
    await queuePage.deleteQueue()

    await overview.clickOnQueuesTab()
    await queuesAndStreams.isLoaded()

    await queuesAndStreams.clickOnQueue("%2F", queue2)
    await queuePage.isLoaded()
    await queuePage.ensureDeleteQueueSectionIsVisible()
    await queuePage.deleteQueue()
  })

  afterEach(async function () {
    await captureScreenshotIfFailed(captureScreen, this)
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
