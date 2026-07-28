const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, delay, idpLoginPage } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')
const OverviewPage = require('../../pageobjects/OverviewPage')

describe('Once an OAuth2 user is logged in', function () {
  let driver
  let homePage
  let idpLogin
  let overview
  let captureScreen
  
  this.timeout(80000)

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    homePage = new SSOHomePage(driver)
    idpLogin = idpLoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('it is forced to log out after the login_session_timeout expires', async function () {
    await homePage.clickToLogin()
    await idpLogin.login('rabbit_admin', 'rabbit_admin')
    await overview.isLoaded()

    // Wait for the 1-minute `login_session_timeout` to expire.
    // In this suite, token TTL is 30s, so it WILL refresh silently in the background
    // around ~20-30s. The hard session timeout should STILL kick us out after 60s
    // regardless of the fact that the token is actively refreshing.
    await delay(62000)
    
    // Attempt an action or wait for the auto-logout to redirect us
    await homePage.isLoaded()
    const value = await homePage.getLoginButton()
    assert.equal(value, 'Click here to log in')
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
