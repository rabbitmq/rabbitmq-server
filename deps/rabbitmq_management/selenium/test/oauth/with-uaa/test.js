const {By,Key,until,Builder} = require("selenium-webdriver");
require("chromedriver");
var assert = require('assert');

var buildDriver = function(caps) {
  return new Builder()
    .forBrowser('chrome')
    .usingServer("http://localhost:4444")
    .build();
};

describe("Management UI with UAA running", function() {
  var driver;
  var page;
  var rabbitmqURL = process.env.RABBITMQ_URL;
  if (!process.env.RABBITMQ_URL) {
    rabbitmqURL = "http://rabbitmq:15672";
  }

  it("should have a title", function() {

      console.log("some tests");

  });

})
