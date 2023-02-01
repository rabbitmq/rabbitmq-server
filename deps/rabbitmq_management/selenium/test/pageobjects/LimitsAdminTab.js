const { By, Key, until, Builder } = require('selenium-webdriver')

const AdminTab = require('./AdminTab')

const SELECTED_LIMITS_ON_RHM = By.css('div#rhs ul li a[href="#/limits"]')

const VIRTUAL_HOST_LIMITS_SECTION = By.css('div#main div#virtual-host-limits')
const USER_LIMITS_SECTION = By.css('div#main div#user-limits')

module.exports = class LimitsAdminTab extends AdminTab {
  async isLoaded () {
    await this.waitForDisplayed(SELECTED_LIMITS_ON_RHM)
  }

  async list_virtual_host_limits() {
    await this.click(VIRTUAL_HOST_LIMITS_SECTION)
  }
  async list_user_limits() {
    await this.click(USER_LIMITS_SECTION)
  }

}
