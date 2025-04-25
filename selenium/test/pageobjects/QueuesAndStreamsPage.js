const { By, Key, until, Builder } = require('selenium-webdriver')

const { delay } = require('../utils')

const BasePage = require('./BasePage')


const PAGING_SECTION = By.css('div#queues-paging-section')
const PAGING_SECTION_HEADER = By.css('div#queues-paging-section h2')
const ADD_NEW_QUEUE_SECTION = By.css('div#add-new-queue')
const FILTER_BY_QUEUE_NAME = By.css('div.filter input#queues-name')

const TABLE_SECTION = By.css('div#queues-table-section table')
const FORM_QUEUE_NAME = By.css('div#add-new-queue form input[name="name"]')
const FORM_QUEUE_TYPE = By.css('div#add-new-queue form select[name="queuetype"]')
const ADD_BUTTON = By.css('div#add-new-queue form input[type=submit]')

module.exports = class QueuesAndStreamsPage extends BasePage {
  async isLoaded () {
    return this.waitForDisplayed(PAGING_SECTION)
  }
  async getPagingSectionHeaderText() {
    return this.getText(PAGING_SECTION_HEADER)
  }
  async getQueuesTable(firstNColumns) {
    return this.getTable(TABLE_SECTION, firstNColumns)
  }
  async clickOnQueue(vhost, name) {
    return this.click(By.css(
      "div#queues-table-section table.list tbody tr td a[href='#/queues/" + vhost + "/" + name + "']"))
  }
  async ensureAddQueueSectionIsVisible() {    
    await this.click(ADD_NEW_QUEUE_SECTION)
    return driver.findElement(ADD_NEW_QUEUE_SECTION).isDisplayed()
  }
  async ensureAllQueuesSectionIsVisible() {    
    await this.click(PAGING_SECTION)
    return driver.findElement(PAGING_SECTION).isDisplayed()
  }  
  async fillInAddNewQueue(queueDetails) {
    await this.selectOptionByValue(FORM_QUEUE_TYPE, queueDetails.type)
    await delay(1000)
    await this.sendKeys(FORM_QUEUE_NAME, queueDetails.name)
    return this.click(ADD_BUTTON)    
  }
  async filterQueues(filterValue) {
    await this.waitForDisplayed(FILTER_BY_QUEUE_NAME)
    return this.sendKeys(FILTER_BY_QUEUE_NAME, filterValue + Key.RETURN)    
  }
}
