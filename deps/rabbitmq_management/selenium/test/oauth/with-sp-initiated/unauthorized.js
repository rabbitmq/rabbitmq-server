const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, idpLoginPage } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')
const OverviewPage = require('../../pageobjects/OverviewPage')

describe('An user without administrator tag', function () {
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

    await homePage.clickToLogin()
    await idpLogin.login('rabbit_no_management', 'rabbit_no_management')

  })

  it('cannot log in into the management ui', async function () {
    if (!await homePage.isLoaded()) {
      throw new Error('Failed to login')
    }
    const visible = await homePage.isWarningVisible()
    assert.ok(visible)
  })

  it('should get "Not authorized" warning message and logout button but no login button', async function(){
    assert.equal('Not authorized', await homePage.getWarning())
    assert.equal('Click here to logout', await homePage.getLogoutButton())
    assert.ok(!await homePage.isBasicAuthSectionVisible())
    assert.ok(!await homePage.isOAuth2SectionVisible())
  })

  describe("After clicking on logout button", function() {

      before(async function () {
          await homePage.clickToLogout()
      })

      it('should get redirected to home page again without error message', async function(){
        const visible = await homePage.isWarningVisible()
        assert.ok(!visible)
      })

  })


  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
