const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToLogin, tokenFor, captureScreensFor, teardown } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')
const FakePortalPage = require('../../pageobjects/FakePortalPage')

describe('A user who accesses the /login URL with a token without scopes for the management UI', function () {
  let captureScreen
  let username
  let password

  before(async function () {
    driver = buildDriver()
    username = process.env.MGT_UNAUTHORIZED_CLIENT_ID_FOR_IDP_INITIATED || 'producer'
    password = process.env.MGT_UNAUTHORIZED_CLIENT_SECRET_FOR_IDP_INITIATED || 'producer_secret'
    
    captureScreen = captureScreensFor(driver, __filename)
    fakePortal = new FakePortalPage(driver)
    homePage = new SSOHomePage(driver)
    await fakePortal.goToHome(username, password)
    if (!await fakePortal.isLoaded()) {
      throw new Error('Failed to load fakePortal')
    }
    await fakePortal.login()
  })

  it('should get a warning message', async function () {
    const message = await homePage.getWarning()
    assert.equal('Not_Authorized', message)
  })
  it('should be presented with a login button to log in', async function () {
    const value = await homePage.getLoginButton()
    assert.equal(value, 'Click here to log in')
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
