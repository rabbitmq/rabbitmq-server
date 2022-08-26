const {By,Key,until,Builder} = require("selenium-webdriver");
require("chromedriver");
var assert = require('assert');
const {buildDriver, goToHome, delay, teardown} = require("../../utils");

var SSOHomePage = require('../../pageobjects/SSOHomePage')
var UAALoginPage = require('../../pageobjects/UAALoginPage')
var OverviewPage = require('../../pageobjects/OverviewPage')

describe("Once user is logged in", function() {
  var homePage;
  var uaaLogin;
  var overview;
  this.timeout(25000); // hard-coded to 25secs because this test requires 25sec to run

  before(async function() {
    driver = buildDriver();
    await goToHome(driver);
    homePage = new SSOHomePage(driver)
    uaaLogin = new UAALoginPage(driver)
    overview = new OverviewPage(driver)
  });

  it("its token is automatically renewed", async function() {
    await homePage.clickToLogin();
    await uaaLogin.login("rabbit_admin", "rabbit_admin");
    await overview.isLoaded()

    await delay(5000)
    await overview.clickOnConnectionsTab()
    await delay(5000) // 10 sec
    await overview.clickOnConnectionsTab()
    await delay(5000) // 15 sec => accessTokenValiditySeconds = 15 sec
    await overview.clickOnChannelsTab()
    await delay(5000) // 20 sec
    await overview.clickOnQueuesTab()

  });

  after(async function() {
    await teardown(driver, this)
  });
})
