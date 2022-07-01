const {By,Key,until,Builder} = require("selenium-webdriver");
require("chromedriver");
var assert = require('assert');
const {buildDriver, goToHome} = require("../../utils");

var SSOHomePage = require('../../pageobjects/SSOHomePage')

describe("An UAA user with administrator tag", function() {
  var driver;
  var homePage;

  before(async function() {
    driver = buildDriver();
    await goToHome(driver);
    homePage = new SSOHomePage(driver)
  });

  it("can log in into the management ui", async function() {
    await homePage.login("rabbit_admin", "rabbit_admin");

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
