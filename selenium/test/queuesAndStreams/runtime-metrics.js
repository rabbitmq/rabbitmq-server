const assert = require('assert')
const { buildDriver, goToHome, goToQueue, captureScreensFor, teardown } = require('../utils')
const { createQueue, deleteQueue, getManagementUrl, basicAuthorization } = require('../mgt-api')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const QueuePage = require('../pageobjects/QueuePage')

describe('Runtime Metrics section', function () {
  let driver, login, overview, queuePage, captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    queuePage = new QueuePage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('management', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
  })

  const QUEUE_TYPES = ['classic', 'quorum', 'stream']

  QUEUE_TYPES.forEach((type) => {
    describe(`given a ${type} queue`, function () {
      let queueName

      before(async function () {
        queueName = 'test_runtime_metrics_' + type + '_' + Math.floor(Math.random() * 1000)
        createQueue(getManagementUrl(), basicAuthorization('management', 'guest'),
          '/', queueName, { 'x-queue-type': type })
        await goToQueue(driver, '/', queueName)
        await queuePage.isLoaded()
      })

      it('should be displayed', async function () {
        assert.ok(await queuePage.isSectionDisplayed('Runtime Metrics'))
      })

      after(async function () {
        deleteQueue(getManagementUrl(), basicAuthorization('management', 'guest'), '/', queueName)
      })
    })
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
