const { By, Key, until, Builder } = require('selenium-webdriver')

const AdminTab = require('./AdminTab')

const SELECTED_POLICIES_ON_RHM = By.css('div#rhs ul li a[href="#/policies"]')

// setup_visibility() appends section-visible/section-invisible to the class
// attribute at render time, so match on class membership, not equality.
const SECTION_CLASS = "contains(concat(' ', normalize-space(@class), ' '), ' section ')"

const USER_POLICIES_SECTION = By.xpath('//div[@id="main"]/div[' + SECTION_CLASS + '][h2[text()="User policies"]]')
const OPERATOR_POLICIES_SECTION = By.xpath('//div[@id="main"]/div[' + SECTION_CLASS + '][h2[text()="Operator policies"]]')

const USER_POLICIES_TABLE = By.xpath('//div[@id="main"]/div[' + SECTION_CLASS + '][h2[text()="User policies"]]//table[contains(@class,"list")]')
const OPERATOR_POLICIES_TABLE = By.xpath('//div[@id="main"]/div[' + SECTION_CLASS + '][h2[text()="Operator policies"]]//table[contains(@class,"list")]')

module.exports = class PoliciesAdminTab extends AdminTab {
  async isLoaded () {
    await this.waitForDisplayed(SELECTED_POLICIES_ON_RHM)
  }

  async listPolicies() {
    await this.ensureSectionIsVisible(USER_POLICIES_SECTION)
    try {
      return this.getTable(USER_POLICIES_TABLE, 5)
    } catch (NoSuchElement) {
      return Promise.resolve([])
    }
  }

  async listOperatorPolicies() {
    await this.ensureSectionIsVisible(OPERATOR_POLICIES_SECTION)
    try {
      return this.getTable(OPERATOR_POLICIES_TABLE, 5)
    } catch (NoSuchElement) {
      return Promise.resolve([])
    }
  }
}
