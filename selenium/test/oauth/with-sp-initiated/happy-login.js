const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, idpLoginPage } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')
const OverviewPage = require('../../pageobjects/OverviewPage')

const management_username = process.env.MANAGEMENT_USERNAME || 'rabbit_admin'
const management_password = process.env.MANAGEMENT_PASSWORD || 'rabbit_admin'

describe('An user with administrator tag', function () {
  let driver
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

  it('can log in into the management ui', async function () {
    await homePage.clickToLogin()
    await idpLogin.login(management_username, management_password)
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    assert.equal(await overview.getUser(), 'User ' + management_username) 
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
  
})