const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, assertAllOptions, hasProfile } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')

describe('Given two oauth resources and basic auth enabled, an unauthenticated user', function () {
  let homePage
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    homePage = new SSOHomePage(driver)
    captureScreen = captureScreensFor(driver, __filename)
    await homePage.isLoaded()
  })

  it('should be presented with a login button to log in using OAuth 2.0', async function () {
    await homePage.getOAuth2Section()
    assert.equal(await homePage.getLoginButton(), 'Click here to log in')
  })

  it('there should be two OAuth resources to choose from', async function () {
    resources = await homePage.getOAuthResourceOptions()
    if (hasProfile("with-resource-label")) {
      assertAllOptions([
        { value : "rabbit_dev", text : "RabbitMQ Development" },
        { value : "rabbit_prod", text : "RabbitMQ Production" }
        ], resources)
    }else {
      assertAllOptions([
        { value : "rabbit_dev", text : "rabbit_dev" },
        { value : "rabbit_prod", text : "rabbit_prod" }
        ], resources)
    }
  })


  it('should be presented with a login button to log in using Basic Auth', async function () {
    await homePage.toggleBasicAuthSection()
    await homePage.getBasicAuthSection()
    assert.equal(await homePage.getBasicAuthLoginButton(), 'Login')
  })

  it('should not have a warning message', async function () {
    const visible = await homePage.isWarningVisible()
    assert.ok(!visible)
  })


  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
