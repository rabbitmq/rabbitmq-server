const { By, Key, until, Builder } = require('selenium-webdriver')
const { delay } = require('../utils')

const AdminTab = require('./AdminTab')


const FEATURE_FLAGS_SECTION = By.css('div#main div#feature-flags')
const FEATURE_FLAGS_TABLE = By.css('div#main div#feature-flags div#ff-table-section table')

const DISABLED_STABLE_WARNING = By.css('div#ff-disabled-stable-warning')
const ENABLE_ALL_STABLE_BUTTON = By.css('button#ff-enable-all-button')

const ACCEPT_ENABLE_EXPERIMENTAL_FEATURE_FLAG = By.css('input#ff-exp-ack-supported-checkbox')
const CONFIRM_ENABLE_EXPERIMENTAL_FEATURE_FLAG = By.css('button#ff-exp-confirm')


module.exports = class FeatureFlagsAdminTab extends AdminTab {
  async isLoaded () {
    await this.waitForDisplayed(FEATURE_FLAGS_SECTION)
  }

  async getAll() {
    await this.ensureSectionIsVisible(FEATURE_FLAGS_SECTION)
    try
    {
      return this.getTable(FEATURE_FLAGS_TABLE, 4)
    } catch (NoSuchElement) {
      return Promise.resolve([])
    }
  }

  async enable(name, isExperimental = false) {
    let state = await this.getState(name)
    if (!await state.isSelected()) {
      await this.click(this.getToggleLocator(name))
      if (isExperimental) {
        const dialog = await this.driver.wait(
          until.elementLocated(By.css('dialog#ff-exp-dialog[open]')), this.timeout)
        await dialog.findElement(ACCEPT_ENABLE_EXPERIMENTAL_FEATURE_FLAG).click()
        await dialog.findElement(CONFIRM_ENABLE_EXPERIMENTAL_FEATURE_FLAG).click()
      }
    }
  }

  async clickOnEnableAllStableFeatureFlags() {
    return this.click(ENABLE_ALL_STABLE_BUTTON)
  }

  async isDisabledStableWarningDisplayed() {
    return this.isDisplayed(DISABLED_STABLE_WARNING)
  }

  async isDisabledStableWarningNotDisplayed() {
    return this.isElementNotVisible(DISABLED_STABLE_WARNING)
  }

  async isEnableAllStableFeatureFlagsEnabled() {
    const button = await this.waitForDisplayed(ENABLE_ALL_STABLE_BUTTON)
    return button.isEnabled()
  }

  async waitUntilEnabled(name) {
    const checkbox = await this.driver.findElement(this.getCheckboxLocator(name))
    return this.driver.wait(until.elementIsSelected(checkbox), this.timeout,
      'Timed out waiting for feature flag ' + name + ' to become enabled', this.polling)
  }

  // Feature flag names may contain dots, e.g. rabbitmq_4.3.0, which a CSS id
  // selector would read as a class. Hence the attribute selectors below.
  getCheckboxLocator(name) {
    return By.css('div#ff-table-section table input[id="ff-checkbox-' + name + '"]')
  }

  // The checkbox itself is display:none, so the toggle label is what a user clicks.
  getToggleLocator(name) {
    return By.css('div#ff-table-section table label[for="ff-checkbox-' + name + '"]')
  }

  async getState(name) {
    return this.driver.findElement(this.getCheckboxLocator(name))
  }

}
