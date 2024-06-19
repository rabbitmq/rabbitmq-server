const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, delay } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')

describe('An user without management tag', function () {
  let homePage
  let idpLogin
  let overview
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    assert.ok(!await login.isPopupWarningDisplayed())
    await login.login('rabbit_no_management', 'rabbit_no_management')
    await !overview.isLoaded()
  })

  it('cannot log in into the management ui', async function () {    
    const visible = await login.isWarningVisible()
    assert.ok(visible)
  })

  it('should get "Login failed" warning message', async function(){
    assert.equal('Login failed', await login.getWarning())
  })

  it('should get popup warning dialog', async function(){
    assert.ok(login.isPopupWarningDisplayed())
    assert.equal('Not_Authorized', await login.getPopupWarning())
  })

  describe("After clicking on popup warning dialog button", function() {

      before(async function () {
          await login.closePopupWarning()
      })

      it('should close popup warning', async function(){
      await delay(1000)
        const visible = await login.isPopupWarningDisplayed()
        assert.ok(!visible)
      })

  })


  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
