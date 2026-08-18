const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, doUntil } = require('../../utils')
const { getAmqpUrl } = require('../../amqp')
const amqplib = require('amqplib')

const LoginPage = require('../../pageobjects/LoginPage')
const OverviewPage = require('../../pageobjects/OverviewPage')
const ConnectionsPage = require('../../pageobjects/ConnectionsPage')
const ConnectionPage = require('../../pageobjects/ConnectionPage')

describe('List AMQP 0-9-1 connections', function () {
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

  it('amqp 0-9-1 connection is listed with all expected fields', async function () {
    amqpConn = await amqplib.connect(getAmqpUrl() + '?frameMax=0')
    await amqpConn.createChannel()

    await overview.clickOnConnectionsTab()

    const table = await doUntil(async function () {
      return connectionsPage.getConnectionsTable()
    }, function (table) {
      return table.length > 0
    }, 6000)
    assert.equal(table.length, 1)

    await connectionsPage.clickOnConnection(2)
    await connectionPage.isLoaded()

    const details = await connectionPage.getDetails()
    const byName = (name) => details.find((fact) => fact.name === name)

    assert.equal('guest', byName('Username').value)
    assert.equal('AMQP 0-9-1', byName('Protocol').value)
    assert.ok(byName('Connected at').value.length > 0)
    assert.ok(byName('Heartbeat').value.length > 0)
    assert.ok(byName('Frame max').value.length > 0)
    assert.ok(byName('Channel limit').value.length > 0)

    assert.ok(await connectionPage.isSectionDisplayed('Channels'))
    assert.ok(await connectionPage.isSectionNotDisplayed('Sessions'))
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
    if (amqpConn) await amqpConn.close()
  })
})
