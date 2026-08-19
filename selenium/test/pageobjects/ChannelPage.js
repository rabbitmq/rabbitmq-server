const { By, Key, until, Builder } = require('selenium-webdriver')

const BasePage = require('./BasePage')


const CHANNEL_NAME = By.css('div#main h1')
const DETAILS_TABLES = By.css('div#main table.facts')

module.exports = class ChannelPage extends BasePage {
  async isLoaded() {
    return this.waitForDisplayed(CHANNEL_NAME)
  }
  async getName() {
    return this.getText(CHANNEL_NAME)
  }
  // Returns [{name, value}] merging every facts table on the page (Connection/Username/Mode,
  // State/Prefetch count, Messages unacknowledged/unconfirmed/uncommitted/Acks uncommitted, ...).
  async getDetails() {
    return this.getFactsTables(DETAILS_TABLES)
  }
}
