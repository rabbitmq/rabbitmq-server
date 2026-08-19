const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, doUntil } = require('../../utils')
const { getAmqpUrl } = require('../../amqp')
const amqplib = require('amqplib')

const LoginPage = require('../../pageobjects/LoginPage')
const OverviewPage = require('../../pageobjects/OverviewPage')
const ConnectionsPage = require('../../pageobjects/ConnectionsPage')
const ConnectionPage = require('../../pageobjects/ConnectionPage')

describe('AMQP 0-9-1 connections with management stats disabled', function () {
  let driver, login, overview, connectionsPage, connectionPage, captureScreen
  let amqpConn

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    connectionsPage = new ConnectionsPage(driver)
    connectionPage = new ConnectionPage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('guest', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
  })

  it('is listed with basic info instead of the stats-disabled error', async function () {
    amqpConn = await amqplib.connect(getAmqpUrl() + '?frameMax=0')
    await amqpConn.createChannel()

    await overview.clickOnConnectionsTab()
    assert.ok(await overview.isPopupWarningNotDisplayed())

    const table = await doUntil(async function () {
      return connectionsPage.getConnectionsTable()
    }, function (rows) {
      return rows.length > 0
    }, 6000)

    assert.ok(table.some((row) => row.some((cell) => cell.includes('guest'))))

    await connectionsPage.clickOnConnection(2)
    await connectionPage.isLoaded()

    assert.ok(await connectionPage.isSectionNotDisplayed('Channels'))
    assert.ok(await connectionPage.isSectionNotDisplayed('Sessions'))
    assert.ok(await connectionPage.isSectionNotDisplayed('Runtime Metrics'))
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
    if (amqpConn) await amqpConn.close()
  })
})
