const { By, Key, until, Builder } = require('selenium-webdriver')

const OverviewPage = require('./OverviewPage')

const ALL_USERS_SECTION = By.css('div#users-section')
const USER_LINK = By.css('div#menu ul#tabs li a[href="#/connections"]')
const FILTER_USER = By.css('input#users-name')
const CHECKBOX_REGEX = By.css('input#filter-regex-mode')
const FILTERED_USER = By.css('span.filter-highlight')

module.exports = class AdminTab extends OverviewPage {
  async isLoaded () {
    await this.waitForDisplayed(ADMIN_TAB)
  }

  async searchForUser(user, regex = false) {

    var filtered_user = By.css('a[href="#/users/' + user + '"]')

    await this.sendKeys(FILTER_USER, user)
    await this.sendKeys(FILTER_USER, Key.RETURN)
    if (regex) {
      await this.click(CHECKBOX_REGEX)
    }
    await this.driver.sleep(250)
    await this.waitForDisplayed(filtered_user)
    return await this.driver.findElement(filtered_user) != undefined
  }

}
