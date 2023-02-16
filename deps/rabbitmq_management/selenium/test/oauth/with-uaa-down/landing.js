const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, teardown, captureScreensFor } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')

describe('A user which accesses management ui without a session', function () {
  let driver
  let homePage
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    homePage = new SSOHomePage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('should have a warning message when UAA is down', async function () {
    await homePage.isLoaded()
    const message = await homePage.getWarning()
    assert.equal(true, message.endsWith('does not appear to be a running OAuth2.0 instance or may not have a trusted SSL certificate'))
  })

  it('should be presented with a login button to log in', async function () {
    await homePage.isLoaded()
    const value = await homePage.getLoginButton()
    assert.equal(value, 'Click here to log in')
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
