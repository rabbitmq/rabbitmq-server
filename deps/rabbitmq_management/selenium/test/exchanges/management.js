const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, delay } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const ExchangesPage = require('../pageobjects/ExchangesPage')
const ExchangePage = require('../pageobjects/ExchangePage')

describe('Exchange management', function () {
  let login
  let exchanges
  let exchange
  let overview
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    exchanges = new ExchangesPage(driver)
    exchange = new ExchangePage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('guest', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    overview.clickOnExchangesTab()
  })

  it('display summary of exchanges', async function () {
    assert.equal("All exchanges (7)", await exchanges.getPagingSectionHeaderText())
  })

  it('list all default exchanges', async function () {
    actual_table = await exchanges.getExchangesTable(3)
    expected_table = [
      ["(AMQP default)", "direct", "D"],
      ["amq.direct", "direct", "D"],
      ["amq.fanout", "fanout", "D"],
      ["amq.headers", "headers", "D"],
      ["amq.match", "headers", "D"],
      ["amq.rabbitmq.trace", "topic",	"D I"],
      ["amq.topic", "topic", "D"]
    ]
    assert.deepEqual(actual_table, expected_table)
  })

  it('view one exchange', async function () {
    await exchanges.clickOnExchange("%2F", "amq.fanout")
    await exchange.isLoaded()
    assert.equal("amq.fanout", await exchange.getName())
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
