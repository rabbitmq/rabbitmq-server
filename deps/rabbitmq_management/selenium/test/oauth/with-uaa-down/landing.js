const {By,Key,until,Builder} = require("selenium-webdriver");
require("chromedriver");
var assert = require('assert');
const {buildDriver, goToHome, teardown, captureScreensFor} = require("../../utils");

var SSOHomePage = require('../../pageobjects/SSOHomePage')

describe("Management UI without UAA running", function() {
  var driver;
  var homePage;

  before(async function() {
    driver = buildDriver();
    await goToHome(driver);
    homePage = new SSOHomePage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  });

  it("should have a warning message", async function() {
      await homePage.isLoaded();
      let message = await homePage.getWarning();
      assert.equal(true, message.endsWith("does not appear to be a running OAuth2.0 instance or may not have a trusted SSL certificate"));
  });

  it("should have a login button to SSO", async function() {
    await homePage.isLoaded();
    let value = await homePage.getLoginButton()
    assert.equal(value, "Single Sign On");
  });

  after(async function() {
    await teardown(driver, this, captureScreen)
  });
})
