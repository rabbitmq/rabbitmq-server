const { By, Key, until, Builder } = require('selenium-webdriver')

const BasePage = require('./BasePage')


const QUEUE_NAME = By.css('div#main h1 b')

const DELETE_SECTION = By.css('div#main div#delete')
const DELETE_BUTTON = By.css('div#main div#delete input[type=submit]')
const FEATURES_TABLE = By.css('table#details-queue-table td#details-queue-features table.mini')
const STATS_CONSUMER_COUNT = By.css('table#details-queue-stats-table td#consumers')

const CONSUMERS_SECTION = By.css('div#queue-consumers-section')
const CONSUMERS_SECTION_TITLE = By.css('div#queue-consumers-section h2')
const CONSUMERS_TABLE = By.css('div#queue-consumers-section table.list#consumers')

module.exports = class QueuePage extends BasePage {
  async isLoaded() {
    return this.waitForDisplayed(QUEUE_NAME)
  }

  async getName() {
    return this.getText(QUEUE_NAME)
  }
  async getConsumerCount() {
    return this.getText(STATS_CONSUMER_COUNT)
  }
  async getFeatures() {
    return this.getTableMini(FEATURES_TABLE)
  }
  async getConsumersSectionTitle() {
    return this.getText(CONSUMERS_SECTION_TITLE)
  }
  async clickOnConsumerSection() {
    return this.click(CONSUMERS_SECTION)
  }
  async getConsumersTable() {
    return this.getPlainTable(CONSUMERS_TABLE)
  }
  async ensureDeleteQueueSectionIsVisible() {    
    await this.click(DELETE_SECTION)
    return this.driver.findElement(DELETE_SECTION).isDisplayed()
  }
  async deleteQueue() {
    await this.click(DELETE_BUTTON)
    return this.acceptAlert()
  }
}
