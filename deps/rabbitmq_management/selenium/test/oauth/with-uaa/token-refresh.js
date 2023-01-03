const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, delay } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')
const UAALoginPage = require('../../pageobjects/UAALoginPage')
const OverviewPage = require('../../pageobjects/OverviewPage')

describe('Once user is logged in', function () {
  let homePage
  let uaaLogin
  let overview
  let captureScreen
  this.timeout(25000) // hard-coded to 25secs because this test requires 25sec to run

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    homePage = new SSOHomePage(driver)
    uaaLogin = new UAALoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('its token is automatically renewed', async function () {
    await homePage.clickToLogin()
    await uaaLogin.login('rabbit_admin', 'rabbit_admin')
    await overview.isLoaded()

    await delay(15000)
    await overview.isLoaded() // still after accessTokenValiditySeconds = 15 sec
    await overview.clickOnConnectionsTab() // and we can still interact with the ui
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
