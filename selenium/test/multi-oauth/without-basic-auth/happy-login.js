const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, idpLoginPage, hasProfile } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')
const OverviewPage = require('../../pageobjects/OverviewPage')

describe('Given there are three oauth resources but two enabled', function () {
  let homePage
  let idpLogin
  let overview
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    homePage = new SSOHomePage(driver)
    idpLogin = idpLoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('dev_user registered in devkeycloak can log in using RabbitMQ Development OAuth 2.0 resource', async function () {
    if (hasProfile("with-resource-label")) {
      await homePage.chooseOauthResource("RabbitMQ Development")
    }else {
      await homePage.chooseOauthResource("rabbit_dev")
    }
    await homePage.clickToLogin()
    await idpLogin.login('dev_user', 'dev_user')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    assert.ok(!await overview.isPopupWarningDisplayed())
    await overview.logout()
  })
  it('prod_user registered in prodkeycloak can log in using RabbitMQ Development OAuth 2.0 resource', async function () {
    if (hasProfile("with-resource-label")) {
      await homePage.chooseOauthResource("RabbitMQ Production")
    }else {
      await homePage.chooseOauthResource("rabbit_prod")
    }
    await homePage.clickToLogin()
    await idpLogin.login('prod_user', 'prod_user')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    assert.ok(!await overview.isPopupWarningDisplayed())
    await overview.logout()
  })


  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
