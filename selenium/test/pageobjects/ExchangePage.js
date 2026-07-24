const { By, Key, until, Builder } = require('selenium-webdriver')

const BasePage = require('./BasePage')


const EXCHANGE_NAME = By.css('div#main h1 b')
const DELETE_EXCHANGE_SECTION_H2 = By.xpath('//h2[text()="Delete this exchange"]')
const DELETE_BUTTON = By.css('form[action="#/exchanges"][method="delete"] input[type=submit]')

module.exports = class ExchangePage extends BasePage {
  async isLoaded() {
    return this.waitForDisplayed(EXCHANGE_NAME)
  }
  async getName() {
    return this.getText(EXCHANGE_NAME)
  }
  async ensureDeleteExchangeSectionIsVisible() {    
    await this.click(DELETE_EXCHANGE_SECTION_H2)
    return this.driver.findElement(DELETE_BUTTON).isDisplayed()
  }
  async deleteExchange() {
    const button = await this.waitForDisplayed(DELETE_BUTTON)
    await this.scrollTo(button)
    await button.click()
    return this.driver.switchTo().alert().accept()
  }
}
