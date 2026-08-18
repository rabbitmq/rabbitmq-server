const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, delay } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const QueuesAndStreamsPage = require('../pageobjects/QueuesAndStreamsPage')
const QueuePage = require('../pageobjects/QueuePage')

describe('Move messages section', function () {
  let driver
  let login
  let queuesAndStreams
  let queuePage
  let overview
  let captureScreen
  let queueName

  before(async function () {
    driver = buildDriver()
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    queuesAndStreams = new QueuesAndStreamsPage(driver)
    queuePage = new QueuePage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  describe('for user with policymaker or administrator tags', function () {
    before(async function () {
      queueName = "test_" + Math.floor(Math.random() * 1000)
      await goToHome(driver)
      await login.login('guest', 'guest')
      if (!await overview.isLoaded()) {
        throw new Error('Failed to login')
      }
      await overview.selectRefreshOption("Do not refresh")
      await overview.clickOnQueuesTab()
      
      await queuesAndStreams.ensureAddQueueSectionIsVisible()
      await queuesAndStreams.fillInAddNewQueue({"name" : queueName, "type" : "classic"})
      await delay(5000)
      await queuesAndStreams.filterQueues(queueName)
      await delay(2000)
      await queuesAndStreams.clickOnQueue("%2F", queueName)
      assert.ok(await queuePage.isLoaded())
    })

    it('should be displayed', async function () {
      assert.ok(await queuePage.isSectionDisplayed("Move messages"))
    })

    after(async function () {
      await queuePage.ensureDeleteQueueSectionIsVisible()
      await queuePage.deleteQueue()
      await overview.logout()
    })
  })

  describe('for user without policymaker or administrator tags', function () {
    before(async function () {
      queueName = "test_" + Math.floor(Math.random() * 1000)
      await goToHome(driver)
      await login.login('management', 'guest')
      if (!await overview.isLoaded()) {
        throw new Error('Failed to login')
      }
      await overview.selectRefreshOption("Do not refresh")
      await overview.clickOnQueuesTab()
      
      await queuesAndStreams.ensureAddQueueSectionIsVisible()
      await queuesAndStreams.fillInAddNewQueue({"name" : queueName, "type" : "classic"})
      await delay(5000)
      await queuesAndStreams.filterQueues(queueName)
      await delay(2000)
      await queuesAndStreams.clickOnQueue("%2F", queueName)
      assert.ok(await queuePage.isLoaded())
    })

    it('should not be displayed', async function () {
      assert.ok(await queuePage.isSectionNotDisplayed("Move messages"))
    })

    after(async function () {
      await queuePage.ensureDeleteQueueSectionIsVisible()
      await queuePage.deleteQueue()
      await overview.logout()
    })
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
