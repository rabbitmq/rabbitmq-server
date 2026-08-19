const { By, Key, until, Builder } = require('selenium-webdriver')

const BasePage = require('./BasePage')


const TABLE_SECTION = By.css('div#main table.list')

module.exports = class ChannelsPage extends BasePage {
  async isLoaded () {
    return this.waitForDisplayed(TABLE_SECTION)
  }
  async getChannelsTable(firstNColumns) {
    return this.getTable(TABLE_SECTION, firstNColumns)
  }
  async clickOnChannel(index) {
    return this.click(By.css(
      "div#main table.list tbody tr td:nth-child(" + index + ")"))
  }
}
