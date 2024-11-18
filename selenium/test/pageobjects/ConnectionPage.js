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
  async getSessions() {
    await this.waitForDisplayed(SESSIONS_SECTION)
    return { 
      sessions : await this.getTable(SESSIONS_TABLE, 100, "session"), 
      incoming_links : await this.getTable(INCOMING_LINKS_TABLE, 100, "link"),
      outgoing_links : await this.getTable(OUTCOMING_LINKS_TABLE, 100, "link") 
    }
  }
  getSessionInfo(sessions, index) {
    return {
      channelNumber: sessions[index][0],
      handleMax: sessions[index][1],
      nextIncomingId: sessions[index][2],      
      incomingWindow: sessions[index][3],
      nextOutgoingId: sessions[index][4],
      remoteIncomingWindow: sessions[index][5],
      remoteOutgoingWindow: sessions[index][6],
      outgoingUnsettledDeliveries: sessions[index][7]
    }
  }
  getIncomingLinkInfo(links, index) {
    return {
      handle: links[index][0],
      name: links[index][1],
      targetAddress: links[index][2],
      sndSettleMode: links[index][3],
      maxMessageSize: Number(links[index][4]),
      deliveryCount: Number(links[index][5]),
      linkCredit: Number(links[index][6]),
      unconfirmedMessages: Number(links[index][7])      
    }
  } 
  getOutgoingLinkInfo(links, index) {
    return {
      handle: links[index][0],
      name: links[index][1],
      sourceAddress: links[index][2],
      queueName: links[index][3],
      sendSettled: links[index][4] == "&#9679;" ? true : false,
      maxMessageSize: links[index][5],      
      deliveryCount: Number(links[index][6]),
      linkCredit: Number(links[index][7])      
    }
  }
}
