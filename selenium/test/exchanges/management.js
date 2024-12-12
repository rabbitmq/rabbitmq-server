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
    assert.equal("All exchanges (15)", await exchanges.getPagingSectionHeaderText())
  })

  it('list all default exchanges', async function () {
<<<<<<< HEAD
    actual_table = await exchanges.getExchangesTable(3)
    console.log("a :" + actual_table)
    expected_table = [
=======
    let actual_table = await exchanges.getExchangesTable(3)
    
    let expected_table = [
>>>>>>> 5086e283b (Allow building CLI with elixir 1.18.x)
      ["/", "(AMQP default)", "direct"],
      ["/", "amq.direct", "direct"],
      ["/", "amq.fanout", "fanout"],
      ["/", "amq.headers", "headers"],
      ["/", "amq.match", "headers"],
      ["/", "amq.rabbitmq.event", "topic"],
      ["/", "amq.rabbitmq.trace", "topic"],
      ["/", "amq.topic", "topic"],
<<<<<<< HEAD

=======
   
>>>>>>> 5086e283b (Allow building CLI with elixir 1.18.x)
      ["other", "(AMQP default)", "direct"],
      ["other", "amq.direct", "direct"],
      ["other", "amq.fanout", "fanout"],
      ["other", "amq.headers", "headers"],
      ["other", "amq.match", "headers"],
      ["other", "amq.rabbitmq.trace", "topic"],
      ["other", "amq.topic", "topic"]
    ]
<<<<<<< HEAD
=======
    
>>>>>>> 5086e283b (Allow building CLI with elixir 1.18.x)
    console.log("e :" + actual_table)
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
