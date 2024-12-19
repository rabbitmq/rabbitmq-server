const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToLogin, tokenFor, captureScreensFor, teardown } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')
const FakePortalPage = require('../../pageobjects/FakePortalPage')

describe('A user who accesses the /login URL with a token without scopes for the management UI', function () {
  let overview
  let captureScreen
  let token

  before(async function () {
    driver = buildDriver()
    captureScreen = captureScreensFor(driver, __filename)
    fakePortal = new FakePortalPage(driver)
    homePage = new SSOHomePage(driver)
    await fakePortal.goToHome('producer', 'producer_secret')
    if (!await fakePortal.isLoaded()) {
      throw new Error('Failed to load fakePortal')
    }
    await fakePortal.login()
  })

  it('should get a warning message', async function () {
    assert.equal(await homePage.getWarning(), 'Not_Authorized')
  })
  it('should be presented with a login button to log in', async function () {
    assert.equal(await homePage.getLoginButton(), 'Click here to log in')
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
