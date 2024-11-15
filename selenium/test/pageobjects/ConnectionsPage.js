const { By, Key, until, Builder } = require('selenium-webdriver')

const BasePage = require('./BasePage')


const PAGING_SECTION = By.css('div#connections-paging-section')
const PAGING_SECTION_HEADER = By.css('div#connections-paging-section h2')

const TABLE_SECTION = By.css('div#connections-table-section table')

module.exports = class ConnectionsPage extends BasePage {
  async isLoaded () {
    return this.waitForDisplayed(PAGING_SECTION)
  }
  async getPagingSectionHeaderText() {
    return this.getText(PAGING_SECTION_HEADER)
  }
  async getConnectionsTable(firstNColumns) {
    return this.getTable(TABLE_SECTION, firstNColumns)
  }
  async clickOnConnection(index) {
    return this.click(By.css(
      "div#connections-table-section table tbody tr td:nth-child(" + index + ")"))
  }
}
