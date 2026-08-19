const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, doUntil } = require('../../utils')
const { getAmqpUrl } = require('../../amqp')
const amqplib = require('amqplib')

const LoginPage = require('../../pageobjects/LoginPage')
const OverviewPage = require('../../pageobjects/OverviewPage')
const ChannelsPage = require('../../pageobjects/ChannelsPage')
const ChannelPage = require('../../pageobjects/ChannelPage')

describe('AMQP 0-9-1 channels with management stats disabled', function () {
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

  it('is listed with basic info instead of the stats-disabled error', async function () {
    amqpConn = await amqplib.connect(getAmqpUrl() + '?frameMax=0')
    await amqpConn.createChannel()

    await overview.clickOnChannelsTab()
    assert.ok(await overview.isPopupWarningNotDisplayed())

    const table = await doUntil(async function () {
      return channelsPage.getChannelsTable()
    }, function (rows) {
      return rows.length > 0
    }, 6000)

    assert.ok(table.some((row) => row.some((cell) => cell.includes('guest'))))
  })

  it('individual channel page shows basic info without the Consumers section', async function () {
    await channelsPage.clickOnChannel(1)
    await channelPage.isLoaded()

    const details = await channelPage.getDetails()
    const byName = (name) => details.find((fact) => fact.name === name)

    assert.ok(byName('Connection').value.length > 0)
    assert.equal('guest', byName('Username').value)

    assert.ok(await channelPage.isSectionNotDisplayed('Consumers'))
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
    if (amqpConn) await amqpConn.close()
  })
})
