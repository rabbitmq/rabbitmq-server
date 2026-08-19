const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, doUntil } = require('../utils')
const { getAmqpUrl } = require('../amqp')
const amqplib = require('amqplib')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const ChannelsPage = require('../pageobjects/ChannelsPage')
const ChannelPage = require('../pageobjects/ChannelPage')

describe('List AMQP 0-9-1 channels', function () {
  let driver, login, overview, channelsPage, channelPage, captureScreen
  let amqpConn

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    channelsPage = new ChannelsPage(driver)
    channelPage = new ChannelPage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('guest', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
  })

  it('amqp 0-9-1 channel is listed with all expected fields', async function () {
    amqpConn = await amqplib.connect(getAmqpUrl() + '?frameMax=0')
    await amqpConn.createChannel()

    await overview.clickOnChannelsTab()

    const table = await doUntil(async function () {
      return channelsPage.getChannelsTable()
    }, function (rows) {
      return rows.length > 0
    }, 6000)
    assert.equal(table.length, 1)

    await channelsPage.clickOnChannel(1)
    await channelPage.isLoaded()

    const details = await channelPage.getDetails()
    const byName = (name) => details.find((fact) => fact.name === name)

    assert.ok(byName('Connection').value.length > 0)
    assert.equal('guest', byName('Username').value)
    // A plain channel (no confirmSelect/txSelect) legitimately has a blank Mode cell.
    assert.notEqual(undefined, byName('Mode'))
    assert.ok(byName('State').value.length > 0)
    assert.equal('0', byName('Prefetch count').value)
    assert.equal('0', byName('Messages unacknowledged').value)
    assert.equal('0', byName('Messages unconfirmed').value)
    assert.equal('0', byName('Messages uncommitted').value)
    assert.equal('0', byName('Acks uncommitted').value)

    assert.ok(await channelPage.isSectionDisplayed('Consumers'))
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
    if (amqpConn) await amqpConn.close()
  })
})
