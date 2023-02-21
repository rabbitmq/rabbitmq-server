const { By, Key, until, Builder } = require('selenium-webdriver')

const BasePage = require('./BasePage')


const PAGING_SECTION = By.css('div#exchanges-paging-section')
const PAGING_SECTION_HEADER = By.css('div#exchanges-paging-section h2')

const TABLE_SECTION = By.css('div#exchanges-table-section table')

module.exports = class ExchangesPage extends BasePage {
  async isLoaded () {
    return this.waitForDisplayed(PAGING_SECTION)
  }
  async getPagingSectionHeaderText() {
    return this.getText(PAGING_SECTION_HEADER)
  }
  async getExchangesTable(firstNColumns) {
    return this.getTable(TABLE_SECTION, firstNColumns)
  }
  async clickOnExchange(vhost, name) {
    return this.click(By.css(
      "div#exchanges-table-section table tbody tr td a[href='#/exchanges/" + vhost + "/" + name + "']"))
  }
}
