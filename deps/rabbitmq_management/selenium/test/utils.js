const {By,Key,until,Builder} = require("selenium-webdriver");
require("chromedriver");

var baseUrl = process.env.RABBITMQ_URL;
var runLocal = process.env.RUN_LOCAL;
if (!process.env.RABBITMQ_URL) {
  baseUrl = "http://local-rabbitmq:15672";
}

module.exports = {
  buildDriver: (caps) => {
    builder = new Builder().forBrowser('chrome');
    if (!runLocal) {
      builder = builder.usingServer("http://selenium:4444/wd/hub")
    }
    return builder.build();
  },

  goToHome: (driver) => {
    return driver.get(baseUrl)
  }

}
