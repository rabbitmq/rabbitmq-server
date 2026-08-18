const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, delay } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const QueuesAndStreamsPage = require('../pageobjects/QueuesAndStreamsPage')
const QueuePage = require('../pageobjects/QueuePage')

describe('Should display stats sections when stats are enabled', function () {
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

  describe('for classic queues', function () {
    before (async function () {
      await queuesAndStreams.ensureAddQueueSectionIsVisible()
      await queuesAndStreams.fillInAddNewQueue({"name" : queueName + "_classic", "type" : "classic"})
      await delay(5000)
      await queuesAndStreams.filterQueues(queueName + "_classic")
      await delay(2000)
    })
    
    it('should display sections that are hidden when stats are disabled', async function () {
      await queuesAndStreams.clickOnQueue("%2F", queueName + "_classic")
      assert.ok(await queuePage.isLoaded())

      assert.ok(await queuePage.isSectionDisplayed("Overview"))
      assert.ok(await queuePage.isSubsectionDisplayed("Queued messages"))
      assert.ok(await queuePage.isSubsectionDisplayed("Message rates"))
      assert.ok(await queuePage.isSubsectionDisplayed("Details"))
      
      // Consumers section is only visible when stats are enabled
      assert.ok(await queuePage.isSectionDisplayed("Consumers"))
      
      // Standard sections
      assert.ok(await queuePage.isSectionDisplayed("Bindings"))
     
    })
    
    after (async function () {
      await queuePage.ensureDeleteQueueSectionIsVisible()
      await queuePage.deleteQueue()
    })
  })

  describe('for quorum queues', function () {
    before (async function () {
      await queuesAndStreams.ensureAddQueueSectionIsVisible()
      await queuesAndStreams.fillInAddNewQueue({"name" : queueName + "_quorum", "type" : "quorum"})
      await delay(5000)
      await queuesAndStreams.filterQueues(queueName + "_quorum")
      await delay(2000)
    })
    
    it('should display sections that are hidden when stats are disabled', async function () {
      await queuesAndStreams.clickOnQueue("%2F", queueName + "_quorum")
      assert.ok(await queuePage.isLoaded())

      assert.ok(await queuePage.isSectionDisplayed("Overview"))
      assert.ok(await queuePage.isSubsectionDisplayed("Queued messages"))
      assert.ok(await queuePage.isSubsectionDisplayed("Message rates"))
      assert.ok(await queuePage.isSubsectionDisplayed("Details"))
      
      // Consumers section is only visible when stats are enabled
      assert.ok(await queuePage.isSectionDisplayed("Consumers"))
      
      // Standard sections
      assert.ok(await queuePage.isSectionDisplayed("Bindings"))
     
    })
    
    after (async function () {
      await queuePage.ensureDeleteQueueSectionIsVisible()
      await queuePage.deleteQueue()
    })
  })

  describe('for streams', function () {
    before (async function () {
      await queuesAndStreams.ensureAddQueueSectionIsVisible()
      await queuesAndStreams.fillInAddNewQueue({"name" : queueName + "_stream", "type" : "stream"})
      await delay(5000)
      await queuesAndStreams.filterQueues(queueName + "_stream")
      await delay(2000)
    })
    
    it('should display sections that are hidden when stats are disabled', async function () {
      await queuesAndStreams.clickOnQueue("%2F", queueName + "_stream")
      assert.ok(await queuePage.isLoaded())

      assert.ok(await queuePage.isSectionDisplayed("Overview"))
      assert.ok(await queuePage.isSubsectionDisplayed("Queued messages"))
      assert.ok(await queuePage.isSubsectionDisplayed("Message rates"))
      assert.ok(await queuePage.isSubsectionDisplayed("Details"))
      
      // Consumers section is only visible when stats are enabled
      assert.ok(await queuePage.isSectionDisplayed("Consumers"))
      
      // Stream publishers section is only visible when stats are enabled (and only for streams)
      // and only if rabbitmq_stream_management plugin is enabled
      const profiles = process.env.PROFILES || ""
      if (!profiles.includes("disable-stream-management")) {
        assert.ok(await queuePage.isSectionDisplayed("Stream publishers"))
      } else {
        assert.ok(await queuePage.isSectionNotDisplayed("Stream publishers"))
      }
      
      // Standard sections
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
