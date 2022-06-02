const {By,Key,until,Builder} = require("selenium-webdriver");
require("chromedriver");
var assert = require('assert');

var buildDriver = function(caps) {
  return new Builder().forBrowser('chrome').build();
};

describe("An UAA user with administrator tag", function() {
  var driver;
  var page;
  this.timeout(3000);

  before(function(done) {
    driver = buildDriver();
    page = driver.get("http://localhost:15672");
    done();
  });

  it("can access management ui", function(done) {
      driver.wait(until.elementLocated(By.id("loginWindow")))
        .then( button => {
            button.click();
            return driver.wait(until.elementLocated(By.name("username")));
        })
        .then( username => {
            username.sendKeys('rabbit_admin');
            return driver.findElement(By.name("password"));
        })
       .then( password => {
            password.sendKeys('rabbit_admin', Key.ENTER);
            return driver.wait(until.elementLocated(By.tagName("h1")));
        })
       .then( h1 => {
            console.log(h1.getText());
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
