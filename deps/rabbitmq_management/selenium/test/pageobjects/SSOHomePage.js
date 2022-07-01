const {By,Key,until,Builder} = require("selenium-webdriver");

var BasePage = require('./BasePage')

const LOGIN_BUTTON = By.css('div#login button#loginWindow');
const USERNAME = By.css('input[name="username"]')
const PASSWORD = By.css('input[name="password"]')

module.exports = class SSOHomePage extends BasePage {

  async isLoaded () {
    await this.waitForDisplayed(LOGIN_BUTTON)
  }
  async getLoginButton() {
    await this.getText(LOGIN_BUTTON)
  }
  async login(username, password) {
    await this.sendKeys(USERNAME, username)
    await this.sendKeys(PASSWORD, password)
    await this.click(LOGIN_BUTTON)
  }
}
