const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToExchanges, captureScreensFor, teardown, goToHome } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')
const UAALoginPage = require('../../pageobjects/UAALoginPage')
const ExchangesPage = require('../../pageobjects/ExchangesPage')

describe('A user which accesses a protected URL without a session', function () {
  let homePage
  let uaaLogin
  let exchanges
  let captureScreen

  before(async function () {
    driver = buildDriver()
    homePage = new SSOHomePage(driver)
    uaaLogin = new UAALoginPage(driver)
    exchanges = new ExchangesPage(driver)

    await goToExchanges(driver)

    captureScreen = captureScreensFor(driver, __filename)
  })

  it('redirect to previous accessed page after login ', async function () {
    await homePage.clickToLogin()

    await uaaLogin.login('rabbit_admin', 'rabbit_admin')

    if (!await exchanges.isLoaded()) {
      throw new Error('Failed to login')
    }

    assert.equal("All exchanges (8)", await exchanges.getPagingSectionHeaderText())
  })


  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
