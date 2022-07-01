const {By,Key,until,Builder} = require("selenium-webdriver");
require("chromedriver");
var assert = require('assert');

var buildDriver = function(caps) {
  return new Builder().forBrowser('chrome').usingServer("http://selenium:4444").build();
};

describe("An UAA user with administrator tag", function() {
  var driver;
  var page;
  this.timeout(10000);
  var rabbitmqURL = process.env.RABBITMQ_URL;

  before(function(done) {
    driver = buildDriver();
    page = driver.get(rabbitmqURL);
    done();
  });

  it("can access the management ui", function(done) {
      driver.wait(until.elementLocated(By.id("loginWindow")))
        .then( button => {
            button.click();
            return driver.wait(until.elementLocated(By.name("username")));
        })
        .then( username => {
            console.log("Entering username & password");
            username.sendKeys('rabbit_admin');
            driver.findElement(By.name("password")).sendKeys('rabbit_admin', Key.ENTER);
            return driver.wait(until.elementLocated(By.id("logout"))).getText();
        })
       .then( logout => {
            console.log("User :" + logout);
            assert.equal("User rabbit_admin", logout);
            done();
       })
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
