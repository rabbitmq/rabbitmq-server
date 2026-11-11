const { By, Key, until, Builder, Select } = require('selenium-webdriver')

const BasePage = require('./BasePage')


const PAGING_SECTION = By.css('div#connections-paging-section')
const PAGING_SECTION_HEADER = By.css('div#connections-paging-section h2')

const TABLE_SECTION = By.css('div#connections-table-section table')
const FILTER = By.css('div.filter input#connections-name')
const FILTER_FIELD = By.css('div.filter select#connections-filter-field')

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
  async filterConnections(field, filterValue) {
    const select = await this.waitForDisplayed(FILTER_FIELD)
    if (await select.getAttribute('value') != field) {
      await new Select(select).selectByValue(field)
      await this.driver.wait(until.stalenessOf(select), this.timeout)
    }
    return this.sendKeys(FILTER, filterValue + Key.RETURN)
  }
  async storeFilterField(field) {
    return this.driver.executeScript(
      "store_pref('connections_current_filter_field', arguments[0])", field)
  }
  async clickOnConnection(index) {
    return this.click(By.css(
      "div#connections-table-section table tbody tr td:nth-child(" + index + ")"))
  }
}
