const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, doUntil } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')

describe('An user without management tag', function () {
  let driver
  let overview
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('rabbit_no_management', 'guest')    
  })

  it('cannot log in into the management ui', async function () {    
    const notVisible = await login.isWarningNotVisible()
    assert.ok(!notVisible)
  })

  it('should get "Login failed" warning message', async function(){
    assert.equal('Login failed', await login.getWarning())
  })

  it('should get popup warning dialog', async function(){
    assert.ok(login.isPopupWarningDisplayed())
    assert.equal('Not management user', await login.getPopupWarning())
  })

  describe("After clicking on popup warning dialog button", function() {

      before(async function () {
          await login.closePopupWarning()
      })

      it('should close popup warning', async function(){
        await doUntil(
          () => login.isPopupWarningNotDisplayed(),
          (v) => !!v,
          500,
          'Popup warning was not closed'
        )
        assert.ok(await login.isPopupWarningNotDisplayed())
      })

  })


  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
