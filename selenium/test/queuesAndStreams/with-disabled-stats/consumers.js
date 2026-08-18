const assert = require('assert')
const { buildDriver, goToHome, goToQueue, captureScreensFor, teardown } = require('../../utils')
const { createQueue, deleteQueue, getManagementUrl, basicAuthorization } = require('../../mgt-api')
const { getAmqpUrl } = require('../../amqp')
const amqplib = require('amqplib')

const LoginPage = require('../../pageobjects/LoginPage')
const OverviewPage = require('../../pageobjects/OverviewPage')
const QueuePage = require('../../pageobjects/QueuePage')

describe('Consumers section', function () {
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

  const QUEUE_TYPES = ['classic', 'quorum']

  QUEUE_TYPES.forEach((type) => {
    describe(`given a ${type} queue with a consumer`, function () {
      let queueName
      let amqpConn

      before(async function () {
        queueName = 'test_consumers_' + type + '_' + Math.floor(Math.random() * 1000)
        createQueue(getManagementUrl(), basicAuthorization('management', 'guest'),
          '/', queueName, { 'x-queue-type': type })

        amqpConn = await amqplib.connect(getAmqpUrl() + '?frameMax=0')
        const channel = await amqpConn.createChannel()
        channel.consume(queueName, (msg) => {}, { consumerTag: 'test-consumer' })

        await goToQueue(driver, '/', queueName)
        await queuePage.isLoaded()
      })

      it('should not be displayed', async function () {
        assert.ok(await queuePage.isSectionNotDisplayed('Consumers'))
      })

      after(async function () {
        if (amqpConn) await amqpConn.close()
        deleteQueue(getManagementUrl(), basicAuthorization('management', 'guest'), '/', queueName)
      })
    })
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
