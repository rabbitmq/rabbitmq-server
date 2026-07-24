const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, captureScreenshotIfFailed, teardown, delay } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const ExchangesPage = require('../pageobjects/ExchangesPage')
const ExchangePage = require('../pageobjects/ExchangePage')

describe('Exchange creation', function () {
  let driver
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

    await login.login('management', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    await overview.selectRefreshOption("Do not refresh")
    await overview.clickOnExchangesTab()
  })

  it('add exchange and view it', async function () {
    await exchanges.ensureAddExchangeSectionIsVisible()
    let exchangeName = "test_exchange_" + Math.floor(Math.random() * 1000)
    await exchanges.fillInAddNewExchange({"name" : exchangeName, "type" : "direct"})
    await delay(2000)
    
    await exchanges.clickOnExchange("%2F", exchangeName)
    await exchange.isLoaded()
    assert.equal(exchangeName, await exchange.getName())

    await exchange.ensureDeleteExchangeSectionIsVisible()
    await exchange.deleteExchange()
  })

  afterEach(async function () {
    await captureScreenshotIfFailed(captureScreen, this)
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
