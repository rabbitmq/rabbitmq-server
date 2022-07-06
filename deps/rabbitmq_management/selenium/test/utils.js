const {By,Key,until,Builder} = require("selenium-webdriver");
require("chromedriver");

var baseUrl = process.env.RABBITMQ_URL;
if (!process.env.RABBITMQ_URL) {
  baseUrl = "http://localhost:15672";
}

module.exports = {
  buildDriver: (caps) => {
    return new Builder()
      .forBrowser('chrome')
    //  .usingServer("http://localhost:4444/wd/hub")
      .build();
  },

  goToHome: (driver) => {
    return driver.get(baseUrl)
  }

}
