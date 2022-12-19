const { By, Key, until, Builder } = require('selenium-webdriver')

const OverviewPage = require('./OverviewPage')

const USER_LINK = By.css('div#menu ul#tabs li a[href="#/connections"]')

module.exports = class AdminTab extends OverviewPage {
  async isLoaded () {
    await this.waitForDisplayed(ADMIN_TAB)
  }

  async hasUser (user) {

  }

}
