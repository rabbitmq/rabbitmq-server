const {By,Key,until,Builder} = require("selenium-webdriver");

var BasePage = require('./BasePage')

const LOGIN_BUTTON = By.css('div#outer div#login div#login-status button#loginWindow');
const USERNAME = By.css('input[name="username"]')
const PASSWORD = By.css('input[name="password"]')
const WARNING = By.css('.warning')

module.exports = class SSOHomePage extends BasePage {

  async isLoaded () {
    return this.waitForDisplayed(LOGIN_BUTTON)
  }
  async getLoginButton() {
    return this.getText(LOGIN_BUTTON)
  }
  async login(username, password) {
    await this.isLoaded();
    if (!this.isWarningVisible()) {
      await this.sendKeys(USERNAME, username)
      await this.sendKeys(PASSWORD, password)
      return this.click(LOGIN_BUTTON)
    }else {
      throw new Error(`Warning message is visible. Idp is down`)
    }
  }
  async isWarningVisible() {
    const message = await this.getText(WARNING)
    return (message != undefined)
  }
  async getWarning() {
    return this.getText(WARNING)
  }

}
