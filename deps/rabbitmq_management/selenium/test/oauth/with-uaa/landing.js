const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')

<<<<<<< HEAD
describe("Management UI with UAA running", function() {
  var driver;
  var homePage;
=======
describe('A user which accesses any protected URL without a session', function () {
  let homePage
  let captureScreen
>>>>>>> 9354397cbf (Support Idp initiated logon in mgt ui with Oauth)

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    homePage = new SSOHomePage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('should be presented with a login button to log in', async function () {
    await homePage.isLoaded()
    const value = await homePage.getLoginButton()
    assert.equal(value, 'Click here to log in')
  })

  it('should not have a warning message', async function () {
    await homePage.isLoaded()
    const visible = await homePage.isWarningVisible()
    assert.ok(!visible)
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
