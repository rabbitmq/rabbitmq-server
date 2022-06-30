const {By,Key,until,Builder} = require("selenium-webdriver");
require("chromedriver");
var assert = require('assert');

var buildDriver = function(caps) {
  return new Builder().forBrowser('chrome').build();
};

describe("When a logged in user", function() {
  var driver;
  var page;
  this.timeout(10000);
  var rabbitmqURL = process.env.RABBITMQ_URL;

  before(function(done) {
    driver = buildDriver();
    page = driver.get(rabbitmqURL);

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

  it("logs out", function(done) {
      driver.wait(until.elementLocated(By.id("logout"))).findElement(By.tagName("input"))
        .then( button => {
            button.click();
            return driver.wait(until.elementLocated(By.id("loginWindow"))).getText()
        })
       .then( text => {
           assert.equal(text, "Click here to log in");
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
