const { By, Key, until, Builder } = require('selenium-webdriver')

const BasePage = require('./BasePage')


const STREAM_NAME = By.css('div#main h1 b')
const DELETE_SECTION = By.css('div#main div#delete')
const DELETE_BUTTON = By.css('div#main div#delete input[type=submit]')


module.exports = class StreamPage extends BasePage {
  async isLoaded() {
    return this.waitForDisplayed(STREAM_NAME)
  }
  async getName() {
    return this.getText(STREAM_NAME)
  }
  async ensureDeleteQueueSectionIsVisible() {    
    await this.click(DELETE_SECTION)
    return driver.findElement(DELETE_SECTION).isDisplayed()
  }
  async deleteStream() {
    await this.click(DELETE_BUTTON)
    return this.acceptAlert()
  }
}
