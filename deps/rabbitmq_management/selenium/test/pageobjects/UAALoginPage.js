const { By, Key, until, Builder } = require('selenium-webdriver')

const BasePage = require('./BasePage')

const FORM = By.css('form')
const USERNAME = By.css('input[name="username"]')
const PASSWORD = By.css('input[name="password"]')

module.exports = class UAALoginPage extends BasePage {
  async isLoaded () {
    return this.waitForDisplayed(FORM)
  }

  async login (username, password) {
    await this.isLoaded()

    await this.sendKeys(USERNAME, username)
    await this.sendKeys(PASSWORD, password)
    return this.submit(FORM)
  }
}
