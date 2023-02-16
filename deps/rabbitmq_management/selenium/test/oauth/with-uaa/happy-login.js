const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')
const UAALoginPage = require('../../pageobjects/UAALoginPage')
const OverviewPage = require('../../pageobjects/OverviewPage')

describe('An user with administrator tag', function () {
  let homePage
  let uaaLogin
  let overview
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    homePage = new SSOHomePage(driver)
    uaaLogin = new UAALoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('can log in into the management ui', async function () {
    await homePage.clickToLogin()
    await uaaLogin.login('rabbit_admin', 'rabbit_admin')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    assert.equal(await overview.getUser(), 'User rabbit_admin')
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
