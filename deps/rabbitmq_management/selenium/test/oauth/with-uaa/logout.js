const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')
const UAALoginPage = require('../../pageobjects/UAALoginPage')
const OverviewPage = require('../../pageobjects/OverviewPage')

describe('When a logged in user', function () {
  let overview
  let homePage
  let captureScreen

<<<<<<< HEAD
  before(async function() {
    driver = buildDriver();
    await goToHome(driver);
=======
  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
>>>>>>> 9354397cbf (Support Idp initiated logon in mgt ui with Oauth)
    homePage = new SSOHomePage(driver)
    uaaLogin = new UAALoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

<<<<<<< HEAD
  it("logs out", async function() {
    await homePage.clickToLogin();
    // await takeAndSaveScreenshot(driver, require('path').basename(__filename), '01-beforeLogin');
    await uaaLogin.login("rabbit_admin", "rabbit_admin");
    // await takeAndSaveScreenshot(driver, require('path').basename(__filename), '02-afterLogin');
=======
  it('logs out', async function () {
    await homePage.clickToLogin()
    await uaaLogin.login('rabbit_admin', 'rabbit_admin')
>>>>>>> 9354397cbf (Support Idp initiated logon in mgt ui with Oauth)
    await overview.isLoaded()
    // await takeAndSaveScreenshot(driver, require('path').basename(__filename), '03-overview');

    await overview.logout()

    await homePage.isLoaded()
<<<<<<< HEAD
    // await takeAndSaveScreenshot(driver, require('path').basename(__filename), '05-afterUaaLogin');
  });
=======
  })
>>>>>>> 9354397cbf (Support Idp initiated logon in mgt ui with Oauth)

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
