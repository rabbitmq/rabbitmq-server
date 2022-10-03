const {By,Key,until,Builder} = require("selenium-webdriver");
require("chromedriver");
var assert = require('assert');
const {buildDriver, goToHome, captureScreensFor, teardown, delay} = require("../../utils");

var SSOHomePage = require('../../pageobjects/SSOHomePage')
var UAALoginPage = require('../../pageobjects/UAALoginPage')
var OverviewPage = require('../../pageobjects/OverviewPage')

describe("Once user is logged in", function() {
  var homePage;
  var uaaLogin;
  var overview;
  var captureScreen;
  this.timeout(65000); // hard-coded to 25secs because this test requires 25sec to run

  before(async function() {
    driver = buildDriver();
    await goToHome(driver);
    homePage = new SSOHomePage(driver)
    uaaLogin = new UAALoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  });

  it("it is logged out when it exceeds logging_session_timeout of 1 minute", async function() {
    await homePage.clickToLogin();
    await uaaLogin.login("rabbit_admin", "rabbit_admin");
    await overview.isLoaded()
    let startTime = new Date();

    // Let the time pass until 50 seconds pass renewing the oauth token
    // by clicking on the connections tab
    for (let elapsed = 0; elapsed < 50000; elapsed+=10000) {
      await delay(10000)
      await overview.clickOnConnectionsTab()
      endTime = new Date()
      var timeDiff = Math.round((endTime - startTime)/1000);
    }
    // And on the last 10 seconds to reach the 1 minute timeout we do
    // not need to renew the token as it expires only after 15 seconds
    // After 1 minute has elapsed, we should be in the homePage
    await delay(10000)
    endTime = new Date()
    var timeDiff = Math.round((endTime - startTime)/1000);
    await homePage.isLoaded()

    // But also, we should be logged out from the Idp (i.e. UAA)
    // The way to assert that is when we click to login, we should be
    // in the UAA login page
    await homePage.clickToLogin();
    await uaaLogin.isLoaded();
  });

  after(async function() {
    await teardown(driver, this, captureScreen)
  });
})
