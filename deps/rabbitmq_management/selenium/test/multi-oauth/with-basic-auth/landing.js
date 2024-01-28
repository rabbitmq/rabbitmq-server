const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')

describe('A user which accesses any protected URL without a session where basic auth is enabled', function () {
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
    assert.ok(await homePage.getOAuth2Section())
    console.log("had oauth section")
    assert.equal(await homePage.getLoginButton(), 'Click here to log in')
  })

  it('there should be two OAuth resources to choose from', async function () {
    resources = await homePage.getOAuthResourceOptions()
    assert.ok(resources.find((resource) =>
      resource.value == "rabbit_dev" && resource.text == "RabbitMQ Development"))
    assert.ok(resources.find((resource) =>
      resource.value == "rabbit_prod" && resource.text == "RabbitMQ Production"))
  })


  it('should be presented with a login button to log in using Basic Auth', async function () {
    await homePage.toggleBasicAuthSection()
    assert.ok(await homePage.isBasicAuthSectionVisible())
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
