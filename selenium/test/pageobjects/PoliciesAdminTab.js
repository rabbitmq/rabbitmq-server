const { By, Key, until, Builder } = require('selenium-webdriver')

const AdminTab = require('./AdminTab')

const SELECTED_POLICIES_ON_RHM = By.css('div#rhs ul li a[href="#/policies"]')

const USER_POLICIES_SECTION = By.css('div#main div#user-policies')
const OPERATOR_POLICIES_SECTION = By.css('div#main div#operator-policies')

const USER_POLICIES_TABLE = By.css('div#main div#user-policies table.list')
const OPERATOR_POLICIES_TABLE = By.css('div#main div#operator-policies table.list')

module.exports = class PoliciesAdminTab extends AdminTab {
  async isLoaded () {
    await this.waitForDisplayed(SELECTED_POLICIES_ON_RHM)
  }

  async listPolicies() {
    await this.ensureSectionIsVisible(USER_POLICIES_SECTION)
    try {
      return await this.getTable(USER_POLICIES_TABLE)
    } catch (NoSuchElement) {
      return Promise.resolve([])
    }
  }

  async listOperatorPolicies() {
    await this.ensureSectionIsVisible(OPERATOR_POLICIES_SECTION)
    try {
      return await this.getTable(OPERATOR_POLICIES_TABLE)
    } catch (NoSuchElement) {
      return Promise.resolve([])
    }
  }
}
