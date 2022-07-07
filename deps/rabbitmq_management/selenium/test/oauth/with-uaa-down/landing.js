const {By,Key,until,Builder} = require("selenium-webdriver");
require("chromedriver");
var assert = require('assert');
const {buildDriver, goToHome} = require("../../utils");

var SSOHomePage = require('../../pageobjects/SSOHomePage')

describe("Management UI with UAA running", function() {
  var driver;
  var homePage;

  before(async function() {
    driver = buildDriver();
    await goToHome(driver);
    homePage = new SSOHomePage(driver)
  });

  it("should have a warning message", async function() {
      await homePage.isLoaded();
      let message = await homePage.getWarning();
      assert.equal(message, "http://local-uaa:8080/uaa does not appear to be a running OAuth2.0 instance or may not have a trusted SSL certificate");
  });

  it("should have a login button to SSO", async function() {
    await homePage.isLoaded();
    let value = await homePage.getLoginButton()
    assert.equal(value, "Single Sign On");

  });

  after(function(done) {
   if (this.currentTest.isPassed) {
      driver.executeScript("lambda-status=passed");
    } else {
      driver.executeScript("lambda-status=failed");
    }
    driver.quit().then(function() {
      done();
    });
  });
})
