const {By,Key,until,Builder} = require("selenium-webdriver");
require("chromedriver");
var assert = require('assert');

var buildDriver = function(caps) {
  return new Builder()
    .forBrowser('chrome')
    .usingServer("http://localhost:4444")
    .build();
};

var baseUrl = process.env.RABBITMQ_URL;
if (!process.env.RABBITMQ_URL) {
  baseUrl = "http://rabbitmq:15672";
}

function goToHome (driver) {
  return driver.get(baseUrl)
}

describe("Management UI with UAA running", function() {
  var driver;
  var page;

  before(async function() {
    driver = buildDriver();
    await goToHome(driver);
  });

  it("should have a title", function() {
      driver.getTitle().then(function(title) {
        assert(title.match("RabbitMQ Management") != null);
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
