const { By, Key, until, Builder } = require('selenium-webdriver')

const OverviewPage = require('./OverviewPage')

const SELECTED_ADMIN_TAB = By.css('div#menu ul#tabs li a.selected[href="#/users"]')

const ALL_USERS_SECTION = By.css('div#users-section')
const USER_LINK = By.css('div#menu ul#tabs li a[href="#/connections"]')

const FILTER_USER = By.css('input#users-name')
const CHECKBOX_REGEX = By.css('input#filter-regex-mode')
const FILTERED_USER = By.css('span.filter-highlight')

// RHM : RIGHT HAND MENU
const USERS_ON_RHM = By.css('div#rhs ul li a[href="#/users"]')
const VHOSTS_ON_RHM = By.css('div#rhs ul li a[href="#/vhosts"]')
const FEATURE_FLAGS_ON_RHM = By.css('div#rhs ul li a[href="#/feature-flags"]')
const POLICIES_ON_RHM = By.css('div#rhs ul li a[href="#/policies"]')
const LIMITS_ON_RHM = By.css('div#rhs ul li a[href="#/limits"]')
const CLUSTER_ON_RHM = By.css('div#rhs ul li a[href="#/cluster-name"]')
const FEDERATION_STATUS_ON_RHM = By.css('div#rhs ul li a[href="#/federation"]')
const FEDERATION_UPSTREAMS_ON_RHM = By.css('div#rhs ul li a[href="#/federation-upstreams"]')
const SHOVEL_STATUS_ON_RHM = By.css('div#rhs ul li a[href="#/shovels"]')
const SHOVEL_MANAGEMENT_ON_RHM = By.css('div#rhs ul li a[href="#/dynamic-shovels"]')
const TOP_PROCESSES_ON_RHM = By.css('div#rhs ul li a[href="#/top"]')
const TOP_ETS_ON_RHM = By.css('div#rhs ul li a[href="#/top/ets"]')
const TRACING_ON_RHM = By.css('div#rhs ul li a[href="#/traces"]')

module.exports = class AdminTab extends OverviewPage {
  async isLoaded () {
    await this.waitForDisplayed(SELECTED_ADMIN_TAB)
  }
  async waitForUsersMenuOption() {
    return this.waitForDisplayed(USERS_ON_RHM)
  }
  async clickOnUsers() {
    await this.click(USERS_ON_RHM)
  }
  async waitForLimitsMenuOption() {
    return this.waitForDisplayed(LIMITS_ON_RHM)
  }
  async clickOnLimits() {
    await this.click(LIMITS_ON_RHM)
  }
  async clickOnVhosts() {
    await this.click(VHOSTS_ON_RHM)
  }
  async waitForVhostsMenuOption() {
    return this.waitForDisplayed(VHOSTS_ON_RHM)
  }
  async clickOnPolicies() {
    await this.click(POLICIES_ON_RHM)
  }
  async waitForPoliciesMenuOption(option) {
    return this.waitForDisplayed(POLICIES_ON_RHM)
  }
  async clickOnFeatureFlags() {
    await this.click(FEATURE_FLAGS_ON_RHM)
  }
  async waitForFeatureFlagsMenuOption() {
    return this.waitForDisplayed(FEATURE_FLAGS_ON_RHM)
  }
  async clickOnCluster() {
    await this.click(CLUSTER_ON_RHM)
  }
  async waitForClusterMenuOption(option) {
    return this.waitForDisplayed(CLUSTER_ON_RHM)
  }
  async clickOnFederationStatus() {
    await this.click(FEDERATION_STATUS_ON_RHM)
  }
  async waitForFederationStatusMenuOption() {
    return this.waitForDisplayed(FEDERATION_STATUS_ON_RHM)
  }
  async clickOnFederationUpstreams() {
    await this.click(FEDERATION_UPSTREAMS_ON_RHM)
  }
  async waitForFederationUpstreamsMenuOption() {
    return this.waitForDisplayed(FEDERATION_UPSTREAMS_ON_RHM)
  }
  async clickOnShovelStatus() {
    await this.click(SHOVEL_STATUS_ON_RHM)
  }
  async waitForShovelStatusMenuOption() {
    return this.waitForDisplayed(SHOVEL_STATUS_ON_RHM)
  }
  async clickOnShovelManagement() {
    await this.click(SHOVEL_MANAGEMENT_ON_RHM)
  }
  async waitForShovelManagementMenuOption() {
    return this.waitForDisplayed(SHOVEL_MANAGEMENT_ON_RHM)
  }
  async clickOnTopProcesses() {
    await this.click(TOP_PROCESSES_ON_RHM)
  }
  async waitForTopProcessesMenuOption() {
    return this.waitForDisplayed(TOP_PROCESSES_ON_RHM)
  }
  async clickOnTopEtsTable() {
    await this.click(TOP_ETS_ON_RHM)
  }
  async waitForTopEtsTableMenuOption() {
    return this.waitForDisplayed(TOP_ETS_ON_RHM)
  }
  async clickOnTracing() {
    await this.click(TRACING_ON_RHM)
  }
  async waitForTracingMenuOption() {
    return this.waitForDisplayed(TRACING_ON_RHM)
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
