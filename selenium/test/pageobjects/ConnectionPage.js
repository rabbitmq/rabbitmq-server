const { By, Key, until, Builder } = require('selenium-webdriver')

const BasePage = require('./BasePage')


const OVERVIEW_SECTION = By.css('div#main div.section#connection-overview-section')
const SESSIONS_SECTION = By.css('div#main div.section#connection-sessions-section')
const SESSIONS_TABLE = By.css('div.section#connection-sessions-section table.list')
const CONNECTION_NAME = By.css('div#main h2')


module.exports = class ConnectionPage extends BasePage {
  async isLoaded() {
    return this.waitForDisplayed(CONNECTION_NAME)
  }
  async getName() {
    return this.getText(CONNECTION_NAME)
  }
  async list_sessions() {
    // maybe ensure the section is expanded
    await this.waitForDisplayed(SESSIONS_SECTION)
    return this.getTable(SESSIONS_TABLE)    
  } 
}
