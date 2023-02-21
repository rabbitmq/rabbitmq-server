const { By, Key, until, Builder } = require('selenium-webdriver')

const AdminTab = require('./AdminTab')

const VHOST_NAME = By.css('div#main h1 b')
const OVERVIEW_SECTION = By.css('div#main div#overview')
const PERMISSIONS_SECTION = By.css('div#main div#permissions')
const TOPIC_PERMISSIONS_SECTION = By.css('div#main div#topic-permissions')


module.exports = class VhostAdminTab extends AdminTab {
  async isLoaded () {
    await this.waitForDisplayed(VHOST_NAME)
    await this.waitForDisplayed(OVERVIEW_SECTION)
    await this.waitForDisplayed(PERMISSIONS_SECTION)
    return this.waitForDisplayed(TOPIC_PERMISSIONS_SECTION)
  }

  async getName() {
    return await this.getText(VHOST_NAME)
  }


}
