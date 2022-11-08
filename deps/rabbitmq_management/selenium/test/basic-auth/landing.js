const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')

describe('Management UI with basic authentication', function () {
  let homePage
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    loginPage = new LoginPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('should have a login form', async function () {
    await loginPage.isLoaded()
    const value = await loginPage.getLoginButton()
    assert.equal(value, 'Login')
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
