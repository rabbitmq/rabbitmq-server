const { By, Key, until, Builder } = require('selenium-webdriver')

const BasePage = require('./BasePage')

const FORM = By.css('div#login form')
const USERNAME = By.css('input[name="username"]')
const PASSWORD = By.css('input[name="password"]')
const LOGIN_BUTTON = By.css('div#outer div#login form input[type=submit]')
const WARNING = By.css('div#outer div#login div#login-status p')

module.exports = class LoginPage extends BasePage {
  async isLoaded () {
    return this.waitForDisplayed(FORM)
  }

  async login (username, password) {
    await this.isLoaded()
    await this.sendKeys(USERNAME, username)
    await this.sendKeys(PASSWORD, password)
    return this.submit(FORM)
  }

  async getLoginButton () {
    return this.getValue(LOGIN_BUTTON)
  }


  async isWarningVisible () {
    try {
      await this.waitForDisplayed(WARNING)
      return Promise.resolve(true)
    } catch (e) {
      return Promise.resolve(false)
    }
  }
  async getWarnings() {
    try
    {
      return driver.findElements(WARNING)
    } catch (NoSuchElement) {
      return Promise.resolve([])
    }
  }
  async getWarning () {
    return this.getText(WARNING)
  }
  
}
