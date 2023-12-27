const { By, Key, until, Builder } = require('selenium-webdriver')

const BasePage = require('./BasePage')


const EXCHANGE_NAME = By.css('div#main h1 b')


module.exports = class ExchangePage extends BasePage {
  async isLoaded() {
    return this.waitForDisplayed(EXCHANGE_NAME)
  }
  async getName() {
    return this.getText(EXCHANGE_NAME)
  }
}
