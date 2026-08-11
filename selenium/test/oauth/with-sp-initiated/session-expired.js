const { By, Key, until, Builder } = require('selenium-webdriver')
const { buildDriver, goToHome, captureScreensFor, teardown, idpLoginPage } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')
const OverviewPage = require('../../pageobjects/OverviewPage')

describe('Once an OAuth2 user is logged in', function () {
  let driver
  let homePage
  let idpLogin
  let overview
  let captureScreen
  
  this.timeout(160000)

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

    // login_session_timeout is one minute (the minimum), so the logout cannot
    // happen before ~60s; poll for it rather than sleeping a fixed time.
    // Refresh the page on every check to force the UI to re-evaluate the
    // session status instead of waiting for the background partial_update
    // interval.
    await homePage.driver.wait(async () => {
      await homePage.refresh()
      const buttons = await homePage.driver.findElements(By.css('div#outer div#login button#login'))
      if (buttons.length === 0) return false
      try {
        return (await buttons[0].getText()) === 'Click here to log in'
      } catch (e) {
        return false
      }
    }, 150000, 'Was not forced to log out after the login_session_timeout expired', 2000)
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
