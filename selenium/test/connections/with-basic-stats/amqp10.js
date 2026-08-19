const assert = require('assert')
const { open: openAmqp, on: onAmqp, close: closeAmqp } = require('../../amqp')
const { buildDriver, goToHome, captureScreensFor, teardown, doUntil } = require('../../utils')

const LoginPage = require('../../pageobjects/LoginPage')
const OverviewPage = require('../../pageobjects/OverviewPage')
const ConnectionsPage = require('../../pageobjects/ConnectionsPage')
const ConnectionPage = require('../../pageobjects/ConnectionPage')

describe('AMQP 1.0 connections with management stats disabled', function () {
  let driver, login, overview, connectionsPage, connectionPage, captureScreen
  let amqp

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
    let untilConnectionEstablished = new Promise((resolve, reject) => {
      onAmqp('connection_open', function () {
        resolve()
      })
    })
    amqp = openAmqp('/queues/my-queue')
    await untilConnectionEstablished

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
    try {
      if (amqp != null) {
        closeAmqp(amqp.connection)
      }
    } catch (error) {
      console.error('Failed to close amqp10 connection due to ' + error)
    }
  })
})
