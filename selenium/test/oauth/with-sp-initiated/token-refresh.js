const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, delay, idpLoginPage } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')
const UAALoginPage = require('../../pageobjects/UAALoginPage')
const KeycloakLoginPage = require('../../pageobjects/KeycloakLoginPage')
const OverviewPage = require('../../pageobjects/OverviewPage')

describe('Once user is logged in', function () {
  let driver
  let homePage
  let idpLogin
  let overview
  let captureScreen
  
  this.timeout(75000)

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    homePage = new SSOHomePage(driver)
    idpLogin = idpLoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('its token is automatically renewed', async function () {
    await homePage.clickToLogin()
    await idpLogin.login('rabbit_admin', 'rabbit_admin')
    await overview.isLoaded()

    await delay(30000)
    // Still loaded after accessTokenValiditySeconds = 30 sec
    await overview.isLoaded()
    // We can still interact with the UI
    await overview.clickOnConnectionsTab()
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
