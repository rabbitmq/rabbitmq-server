const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, captureScreenshotIfFailed, teardown, delay } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const ExchangesPage = require('../pageobjects/ExchangesPage')
const ExchangePage = require('../pageobjects/ExchangePage')

describe('Exchanges Paging', function () {
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

  it('can navigate through paginated exchanges', async function () {
    await exchanges.ensureAddExchangeSectionIsVisible()

    // Create 2 exchanges to test pagination with page size = 1
    const prefix = "page_test_ex_" + Math.floor(Math.random() * 1000) + "_"
    const exchange1 = prefix + "1"
    const exchange2 = prefix + "2"

    await exchanges.fillInAddNewExchange({"name" : exchange1, "type" : "direct"})
    await delay(1000)
    await exchanges.fillInAddNewExchange({"name" : exchange2, "type" : "direct"})
    await delay(2000)

    // We don't use filtering here, just verify that paging works by checking
    // that page 1 and page 2 show different items when page size is 1.

    // Change page size to 1
    await exchanges.setPageSize(1)
    await delay(2000)

    // Verify only 1 exchange is visible now
    let table = await exchanges.getExchangesTable(2)
    assert.equal(1, table.length)
    const firstExchangeName = table[0][1]

    // Navigate to page 2
    await exchanges.selectPage(2)
    await delay(2000)

    // Verify a different exchange is visible
    table = await exchanges.getExchangesTable(2)
    assert.equal(1, table.length)
    const secondExchangeName = table[0][1]
    assert.notEqual(firstExchangeName, secondExchangeName)

    // Clean up
    // Set page size to a large number so we can find and click our test exchanges
    await exchanges.setPageSize(100)
    await delay(2000)

    await exchanges.clickOnExchange("%2F", exchange1)
    await exchange.isLoaded()
    await exchange.ensureDeleteExchangeSectionIsVisible()
    await exchange.deleteExchange()

    await overview.clickOnExchangesTab()
    await delay(1000)
    
    await exchanges.clickOnExchange("%2F", exchange2)
    await exchange.isLoaded()
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
