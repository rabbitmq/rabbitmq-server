const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, idpLoginPage } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')
const OverviewPage = require('../../pageobjects/OverviewPage')
const FakePortalPage = require('../../pageobjects/FakePortalPage')

describe('When there two OAuth resources', function () {
  let homePage
  let idpLogin
  let overview
  let fakePortal
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    homePage = new SSOHomePage(driver)
    idpLogin = idpLoginPage(driver, "keycloak")
    overview = new OverviewPage(driver)
    fakePortal = new FakePortalPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('can log using first resource (sp_initiated)', async function () {
    await homePage.chooseOauthResource('RabbitMQ Production')
    await homePage.clickToLogin()
    await idpLogin.login('prod_user', 'prod_user')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    assert.equal(await overview.getUser(), 'User prod_user')
    await overview.logout()
  })

  it('can log using third resource (idp_initiated)', async function () {
    if (!await homePage.isLoaded()) {
      throw new Error('Failed to load home page')
    }
    await homePage.chooseOauthResource('RabbitMQ X_Idp')
    await homePage.clickToLogin()
    if (!await fakePortal.isLoaded()) {
      throw new Error('Failed to redirect to IDP')
    }
  })
/* put back once webdriver fixes a known issue
  it('can log using second resource (sp_initiated)', async function () {
    if (!await homePage.isLoaded()) {
      throw new Error('Failed to load home page')
    }
    await homePage.chooseOauthResource('RabbitMQ Production')
    await homePage.clickToLogin()
    await idpLogin.login('prod_user', 'prod_user')

    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    assert.equal(await overview.getUser(), 'User prod_user')
    await overview.logout()

  })
*/

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
