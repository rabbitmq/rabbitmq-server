const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, idpLoginPage } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')
const OverviewPage = require('../../pageobjects/OverviewPage')

describe('When a logged in user', function () {
  let overview
  let homePage
  let captureScreen
  let idpLogin

  before(async function () {
    driver = buildDriver()
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
    await goToHome(driver);
    await overview.isLoaded()
    assert.equal(await overview.getUser(), 'User rabbit_idp_user')    
  })

  it('logs out', async function () {
    await homePage.clickToLogin()
    await idpLogin.login('rabbit_admin', 'rabbit_admin')
    await overview.isLoaded()
    await overview.logout()
    await homePage.isLoaded()

  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
