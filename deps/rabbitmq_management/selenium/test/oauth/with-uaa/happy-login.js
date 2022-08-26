const {By,Key,until,Builder} = require("selenium-webdriver");
require("chromedriver");
var assert = require('assert');
const {buildDriver, goToHome, teardown} = require("../../utils");

var SSOHomePage = require('../../pageobjects/SSOHomePage')
var UAALoginPage = require('../../pageobjects/UAALoginPage')
var OverviewPage = require('../../pageobjects/OverviewPage')

describe("An UAA user with administrator tag", function() {
  var homePage;
  var uaaLogin;
  var overview;

  before(async function() {
    driver = buildDriver();
    await goToHome(driver);
    homePage = new SSOHomePage(driver);
    uaaLogin = new UAALoginPage(driver);
    overview = new OverviewPage(driver);
  });

  it("can log in into the management ui", async function() {
    await homePage.clickToLogin();
    await uaaLogin.login("rabbit_admin", "rabbit_admin");
    if (! await overview.isLoaded()) {
      throw new Error("Failed to login");
    }
    assert.equal(await overview.getUser(), "User rabbit_admin");
  });

  after(async function() {
    await teardown(driver, this)
  });
})
