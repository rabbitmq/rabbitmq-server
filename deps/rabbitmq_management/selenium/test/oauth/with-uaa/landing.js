const {By,Key,until,Builder} = require("selenium-webdriver");
require("chromedriver");
var assert = require('assert');
const {buildDriver, goToHome, teardown} = require("../../utils");

var SSOHomePage = require('../../pageobjects/SSOHomePage')

describe("Management UI with UAA running", function() {
  var driver;
  var homePage;

  before(async function() {
    driver = buildDriver();
    await goToHome(driver);
    homePage = new SSOHomePage(driver)
  });

  it("should have a login button to SSO", async function() {
    await homePage.isLoaded();
    let value = await homePage.getLoginButton()
    assert.equal(value, "Click here to log in");
  });

  it("should not have a warning message", async function() {
    await homePage.isLoaded();
    let visible = await homePage.isWarningVisible();
    assert.ok(!visible);
  });

  after(async function() {
    await teardown(driver, this)
  });
})
