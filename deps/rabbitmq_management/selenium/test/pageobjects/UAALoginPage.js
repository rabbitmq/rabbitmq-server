const {By,Key,until,Builder} = require("selenium-webdriver");

var BasePage = require('./BasePage')

const LOGIN_BUTTON = By.css('div#outer div#login div#login-status button#loginWindow');
const USERNAME = By.css('input[name="username"]')
const PASSWORD = By.css('input[name="password"]')

module.exports = class UAAHomePage extends BasePage {

  async isLoaded () {
  //  return this.waitForDisplayed(LOGIN_BUTTON)
    return true
  }
  async login(username, password) {
    await this.isLoaded();

    await this.sendKeys(USERNAME, username)
    await this.sendKeys(PASSWORD, password)
    return this.click(LOGIN_BUTTON)
  }

}
