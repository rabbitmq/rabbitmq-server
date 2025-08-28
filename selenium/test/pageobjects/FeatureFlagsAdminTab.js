const { By, Key, until, Builder } = require('selenium-webdriver')
const { delay } = require('../utils')

const AdminTab = require('./AdminTab')


const FEATURE_FLAGS_SECTION = By.css('div#main div#feature-flags')
const FEATURE_FLAGS_TABLE = By.css('div#main div#feature-flags div#ff-table-section table')

const ACCEPT_ENABLE_EXPERIMENTAL_FEATURE_FLAG = By.css('p#ff-exp-ack-supported')
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
      await this.driver.findElement(this.getParentCheckboxLocator(name)).click()
      if (isExperimental) {
        await delay(1000)
        const dialog = await this.driver.wait(
          until.elementLocated(By.css('dialog#ff-exp-dialog[open]')),
          10000 // 10 seconds timeout
        )
        await dialog.findElement(ACCEPT_ENABLE_EXPERIMENTAL_FEATURE_FLAG).click()
        await dialog.findElement(CONFIRM_ENABLE_EXPERIMENTAL_FEATURE_FLAG).click()      
        return delay(1000)
      }else {
        return Promise.resolve()  
      }
    }else {
      return Promise.resolve()
    }
  }
  getCheckboxLocator(name) {
    return By.css('div#ff-table-section table input#ff-checkbox-' + name)
  }
  getParentCheckboxLocator(name) {
    return By.css('div#ff-table-section table td#ff-td-' + name)
  }
  async getState(name) {
    return this.driver.findElement(this.getCheckboxLocator(name))
  }

}
