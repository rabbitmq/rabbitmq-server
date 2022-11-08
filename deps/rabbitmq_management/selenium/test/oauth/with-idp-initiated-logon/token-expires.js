const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToLogin, tokenFor, captureScreensFor, teardown, delay } = require('../../utils')

const OverviewPage = require('../../pageobjects/OverviewPage')
const SSOHomePage = require('../../pageobjects/SSOHomePage')
const FakePortalPage = require('../../pageobjects/FakePortalPage')

describe('Once user logs in with its own token', function () {
  let overview
  let homePage
  let fakePortal
  let captureScreen
  this.timeout(17000)

  before(async function () {
    driver = buildDriver()
    fakePortal = new FakePortalPage(driver)
    homePage = new SSOHomePage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await fakePortal.goToHome()
    if (!await fakePortal.isLoaded()) {
      throw new Error('Failed to load fakePortal')
    }
    await fakePortal.login()
    await overview.isLoaded()
  })

  describe('when the token expires', function () {
    before(async function () {
      await delay(15000)
    })

    it('user should be presented with a login button to log in', async function () {
      await homePage.isLoaded()
      const value = await homePage.getLoginButton()
      assert.equal(value, 'Click here to log in')
    })

    after(async function () {
      await teardown(driver, this, captureScreen)
    })
  })
  
})
