const assert = require('assert')
const { buildDriver, goToHome, goToQueue, captureScreensFor, teardown } = require('../utils')
const { createQueue, deleteQueue, getManagementUrl, basicAuthorization } = require('../mgt-api')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const QueuePage = require('../pageobjects/QueuePage')

describe('Purge section', function () {
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

  async function createAndVisitQueue(type, prefix) {
    const queueName = prefix + '_' + type + '_' + Math.floor(Math.random() * 1000)
    createQueue(getManagementUrl(), basicAuthorization('management', 'guest'),
      '/', queueName, { 'x-queue-type': type })
    await goToQueue(driver, '/', queueName)
    await queuePage.isLoaded()
    return queueName
  }

  const PURGEABLE_TYPES = ['classic', 'quorum']
  const NON_PURGEABLE_TYPES = ['stream']

  describe('for a queue type that supports purging', function () {
    PURGEABLE_TYPES.forEach((type) => {
      describe(`given a ${type} queue`, function () {
        let queueName

        before(async function () {
          queueName = await createAndVisitQueue(type, 'test_purge')
        })

        it('should be displayed', async function () {
          assert.ok(await queuePage.isSectionDisplayed('Purge'))
        })

        after(async function () {
          deleteQueue(getManagementUrl(), basicAuthorization('management', 'guest'), '/', queueName)
        })
      })
    })
  })

  describe('for a queue type that does not support purging', function () {
    NON_PURGEABLE_TYPES.forEach((type) => {
      describe(`given a ${type} queue`, function () {
        let queueName

        before(async function () {
          queueName = await createAndVisitQueue(type, 'test_purge')
        })

        it('should not be displayed', async function () {
          assert.ok(await queuePage.isSectionNotDisplayed('Purge'))
        })

        after(async function () {
          deleteQueue(getManagementUrl(), basicAuthorization('management', 'guest'), '/', queueName)
        })
      })
    })
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
