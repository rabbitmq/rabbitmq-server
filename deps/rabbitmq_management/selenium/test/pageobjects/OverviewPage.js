const {By,Key,until,Builder} = require("selenium-webdriver");

var BasePage = require('./BasePage')

const MENU_TABS = By.css('div#menu ul#tabs');
const USER = By.css('li#logout')
const LOGOUT_FORM = By.css('li#logout form')

const CONNECTION_TAB = By.css('div#menu ul#tabs li a[href="#/connections"]');
const CHANNELS_TAB = By.css('div#menu ul#tabs li a[href="#/channels"]');
const QUEUES_TAB = By.css('div#menu ul#tabs li a[href="#/queues"]');
const ADMIN_TAB = By.css('div#menu ul#tabs li a[href="#/users"]');


module.exports = class OverviewPage extends BasePage {

  async isLoaded () {
    return await this.waitForDisplayed(MENU_TABS)
  }
  async logout() {
    await this.submit(LOGOUT_FORM);
  }
  async getUser() {
    return this.getText(USER)
  }
  async clickOnConnectionsTab() {
    return this.click(CONNECTION_TAB)
  }
  async clickOnAdminTab() {
    return this.click(ADMIN_TAB)
  }
  async clickOnChannelsTab() {
    return this.click(CHANNELS_TAB)
  }
  async clickOnQueuesTab() {
    return this.click(QUEUES_TAB)
  }
}
