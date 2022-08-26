const {By,Key,until,Builder} = require("selenium-webdriver");
require("chromedriver");
var assert = require('assert');
const {buildDriver, goToHome, captureScreensFor, teardown} = require("../../utils");

var SSOHomePage = require('../../pageobjects/SSOHomePage')
var UAALoginPage = require('../../pageobjects/UAALoginPage')
var OverviewPage = require('../../pageobjects/OverviewPage')

describe("When a logged in user", function() {
  var overview
  var homePage
  var captureScreen

  before(async function() {
    driver = buildDriver();
    await goToHome(driver);
    homePage = new SSOHomePage(driver)
    uaaLogin = new UAALoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  });

  it("logs out", async function() {
    await homePage.clickToLogin();
    // await takeAndSaveScreenshot(driver, require('path').basename(__filename), '01-beforeLogin');
    await uaaLogin.login("rabbit_admin", "rabbit_admin");
    // await takeAndSaveScreenshot(driver, require('path').basename(__filename), '02-afterLogin');
    await overview.isLoaded()
    // await takeAndSaveScreenshot(driver, require('path').basename(__filename), '03-overview');

    await overview.logout()

    await uaaLogin.isLoaded()
    // await takeAndSaveScreenshot(driver, require('path').basename(__filename), '05-afterUaaLogin');
  });

  after(async function() {
    await teardown(driver, this, captureScreen)
  });
})
