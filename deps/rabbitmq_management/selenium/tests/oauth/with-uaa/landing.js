const {By,Key,until,Builder} = require("selenium-webdriver");
require("chromedriver");
var assert = require('assert');

var buildDriver = function(caps) {
  return new Builder().forBrowser('chrome').build();
};

describe("Management UI with UAA running", function() {
  var driver;
  var page;
  this.timeout(3000);

  before(function(done) {
    driver = buildDriver();
    page = driver.get("http://localhost:15672");
    done();
  });

  it("should have a title", function(done) {
      page.then(function() {
        driver.getTitle().then(function(title) {
          assert(title.match("RabbitMQ Management") != null);
          done();
      });
    });
  });

  it("should have a login button", function(done) {
      console.log("here");
      driver.wait(until.elementLocated(By.id("loginWindow"))).then(function(loginButton) {
        loginButton.getText().then(function(text) {
          assert.equal(text, "Click here to log in");
          done();
        })
    });

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
