const { By, Key, until, Builder } = require('selenium-webdriver')

const BasePage = require('./BasePage')


const OVERVIEW_SECTION = By.css('div#main div.section#connection-overview-section')
const SESSIONS_SECTION = By.css('div#main div.section#connection-sessions-section')
const SESSIONS_TABLE = By.css('div.section#connection-sessions-section table.list#sessions')
const INCOMING_LINKS_TABLE = By.css('div.section#connection-sessions-section table.list#incoming-links')
const OUTCOMING_LINKS_TABLE = By.css('div.section#connection-sessions-section table.list#outgoing-links')
const CONNECTION_NAME = By.css('div#main h2')


module.exports = class ConnectionPage extends BasePage {
  async isLoaded() {
    return this.waitForDisplayed(CONNECTION_NAME)
  }
  async getName() {
    return this.getText(CONNECTION_NAME)
  }
  async list_sessions() {
    await this.waitForDisplayed(SESSIONS_SECTION)
    return { 
      sessions : await this.getTable(SESSIONS_TABLE, 100, "session"), 
      incoming_links : await this.getTable(INCOMING_LINKS_TABLE, 100, "link"),
      outgoing_links : await this.getTable(OUTCOMING_LINKS_TABLE, 100, "link") 
    }
  } 
}
