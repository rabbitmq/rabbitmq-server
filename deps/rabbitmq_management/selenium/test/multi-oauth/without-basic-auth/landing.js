const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')

describe('A user which accesses any protected URL without a session where basic auth is disabled', function () {
  let homePage
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    homePage = new SSOHomePage(driver)
    captureScreen = captureScreensFor(driver, __filename)
    await homePage.isLoaded()
  })

  it('should not be presented with a login button to log in using Basic Auth', async function () {
    assert.ok(!await homePage.isBasicAuthSectionVisible())
  })

  it('should not have a warning message', async function () {
    const visible = await homePage.isWarningVisible()
    assert.ok(!visible)
  })


  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
