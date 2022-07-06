const {By,Key,until,Builder} = require("selenium-webdriver");

var BasePage = require('./BasePage')

const MENU_TABS = By.css('div#menu ul#tabs');
const USER = By.css('li#logout')
const LOGOUT_FORM = By.css('li#logout form')

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

}
