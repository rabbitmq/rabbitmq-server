const {By,Key,until,Builder} = require("selenium-webdriver");
require("chromedriver");
var assert = require('assert');

var buildDriver = function(caps) {
  return new Builder().forBrowser('chrome').build();
};

describe("Management UI with UAA down", function() {
  var driver;
  var page;
  this.timeout(3000);

  before(function(done) {
    driver = buildDriver();
    page = driver.get("http://localhost:15672");
    done();
  });

  it("should have a warning message", function(done) {

      driver.wait(until.elementLocated(By.id("outer")))
        .then(function(outer) { return outer.findElement(By.className("warning")); })
        .then(function(warning) {
          warning.getText().then(function(text) {
            assert.equal(text, "http://localhost:8080/uaa does not appear to be a running OAuth2.0 instance or may not have a trusted SSL certificate");
            done();
          });
        });

  });


  it("should have a login button", function(done) {
      driver.wait(until.elementLocated(By.id("loginWindow"))).then(function(loginButton) {
        loginButton.getText().then(function(text) {
          assert.equal(text, "Single Sign On");
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
