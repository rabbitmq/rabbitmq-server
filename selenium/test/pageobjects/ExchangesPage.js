const { By, Key, until, Builder } = require('selenium-webdriver')

const BasePage = require('./BasePage')


const PAGING_SECTION = By.css('div#exchanges-paging-section')
const PAGING_SECTION_HEADER = By.css('div#exchanges-paging-section h2')
const ADD_NEW_EXCHANGE_SECTION = By.css('div#add-new-exchange')
const FORM_EXCHANGE_NAME = By.css('div#add-new-exchange form input[name="name"]')
const FORM_EXCHANGE_TYPE = By.css('div#add-new-exchange form select[name="type"]')
const ADD_BUTTON = By.css('div#add-new-exchange form input[type=submit]')

const TABLE_SECTION = By.css('div#exchanges-table-section table')
const FILTER_BY_EXCHANGE_NAME = By.css('div.filter input#exchanges-name')
const PAGE_SIZE_INPUT = By.css('input#exchanges-pagesize')
const PAGE_SELECT = By.css('select#exchanges-page')

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
  async ensureAddExchangeSectionIsVisible() {    
    await this.click(ADD_NEW_EXCHANGE_SECTION)
    return this.driver.findElement(ADD_NEW_EXCHANGE_SECTION).isDisplayed()
  }
  async fillInAddNewExchange(exchangeDetails) {
    await this.selectOptionByValue(FORM_EXCHANGE_TYPE, exchangeDetails.type)
    await this.sendKeys(FORM_EXCHANGE_NAME, exchangeDetails.name)
    return this.click(ADD_BUTTON)    
  }
  async filterExchanges(filterValue) {
    await this.waitForDisplayed(FILTER_BY_EXCHANGE_NAME)
    return this.sendKeys(FILTER_BY_EXCHANGE_NAME, filterValue + Key.RETURN)    
  }
  async setPageSize(size) {
    const input = await this.waitForDisplayed(PAGE_SIZE_INPUT)
    await input.clear()
    await this.sendKeys(PAGE_SIZE_INPUT, size + Key.RETURN)
  }
  async selectPage(pageNumber) {
    await this.selectOptionByValue(PAGE_SELECT, String(pageNumber))
  }
}
