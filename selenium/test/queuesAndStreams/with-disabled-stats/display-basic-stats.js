const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, delay } = require('../../utils')

const LoginPage = require('../../pageobjects/LoginPage')
const OverviewPage = require('../../pageobjects/OverviewPage')
const QueuesAndStreamsPage = require('../../pageobjects/QueuesAndStreamsPage')
const QueuePage = require('../../pageobjects/QueuePage')

describe('Should display basic stats even when stats are disabled', function () {
  let driver
  let login
  let queuesAndStreams
  let queuePage
  let overview
  let captureScreen
  let queueName

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    queuesAndStreams = new QueuesAndStreamsPage(driver)
    queuePage = new QueuePage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('guest', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    await overview.selectRefreshOption("Do not refresh")
    await overview.clickOnQueuesTab()
    
    queueName = "test_" + Math.floor(Math.random() * 1000)
  })

  describe('Main tabs are still operational without any warnings', function () {
    it('should load all main tabs without warnings', async function () {
      await overview.clickOnOverviewTab()
      assert.ok(await overview.isLoaded())
      assert.ok(await overview.isPopupWarningNotDisplayed())

      await overview.clickOnConnectionsTab()
      assert.ok(await overview.isPopupWarningNotDisplayed())

      await overview.clickOnChannelsTab()
      assert.ok(await overview.isPopupWarningNotDisplayed())

      await overview.clickOnExchangesTab()
      assert.ok(await overview.isPopupWarningNotDisplayed())

      await overview.clickOnQueuesTab()
      assert.ok(await overview.isPopupWarningNotDisplayed())

      await overview.clickOnAdminTab()
      assert.ok(await overview.isPopupWarningNotDisplayed())
    })
  })

  describe('for classic queues', function () {
    before (async function () {
      await overview.clickOnQueuesTab()
      await queuesAndStreams.ensureAddQueueSectionIsVisible()
      await queuesAndStreams.fillInAddNewQueue({"name" : queueName, "type" : "classic"})
      await delay(5000)
      await queuesAndStreams.filterQueues(queueName)
      await delay(2000)
    })
    it('should display basic stats', async function () {
      await queuesAndStreams.clickOnQueue("%2F", queueName)
      assert.ok(await queuePage.isLoaded())

      assert.ok(await queuePage.isSectionDisplayed("Overview"))
      assert.ok(await queuePage.isSubsectionDisplayed("Details"))
      assert.ok(await queuePage.isSectionNotDisplayed("Message rates breakdown"))      
      assert.ok(await queuePage.isSectionDisplayed("Bindings"))
    })
    
    after (async function () {
      await queuePage.ensureDeleteQueueSectionIsVisible()
      await queuePage.deleteQueue()
    })
  })

  after(async function () {

    await teardown(driver, this, captureScreen)
  })
})
