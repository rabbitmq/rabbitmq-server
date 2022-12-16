const { By, Key, until, Builder } = require('selenium-webdriver')

const BasePage = require('./BasePage')

const MENU_TABS = By.css('div#menu ul#tabs')
const USER = By.css('li#logout')
const LOGOUT_FORM = By.css('li#logout form')

const CONNECTION_TAB = By.css('div#menu ul#tabs li a[href="#/connections"]')
const CHANNELS_TAB = By.css('div#menu ul#tabs li a[href="#/channels"]')
const QUEUES_TAB = By.css('div#menu ul#tabs li a[href="#/queues"]')
const ADMIN_TAB = By.css('div#menu ul#tabs li a[href="#/users"]')

const UPLOAD_DEFINITIONS_SECTION = By.css('div#upload-definitions-section')
const CHOOSE_BROKER_UPLOAD_FILE = By.css('input[name="file"]')
const UPLOAD_BROKER_FILE = By.css('input[type=submit][name="upload-definitions"]')
const POP_UP = By.css('div.form-popup-info')

const DOWNLOAD_DEFINITIONS_SECTION = By.css('div#download-definitions-section')
const CHOOSE_BROKER_DOWNLOAD_FILE = By.css('input#download-filename')
const DOWNLOAD_BROKER_FILE = By.css('button#upload-definitions')

module.exports = class OverviewPage extends BasePage {
  async isLoaded () {
    return await this.waitForDisplayed(MENU_TABS)
  }

  async logout () {
    await this.submit(LOGOUT_FORM)
  }

  async getUser () {
    return this.getText(USER)
  }

  async clickOnConnectionsTab () {
    return this.click(CONNECTION_TAB)
  }

  async clickOnAdminTab () {
    return this.click(ADMIN_TAB)
  }

  async clickOnChannelsTab () {
    return this.click(CHANNELS_TAB)
  }

  async clickOnQueuesTab () {
    return this.click(QUEUES_TAB)
  }

  async uploadBrokerDefinitions(file) {
    await this.click(UPLOAD_DEFINITIONS_SECTION)
    await this.chooseFile(CHOOSE_BROKER_UPLOAD_FILE, file)
    await this.driver.sleep(1000)
    await this.click(UPLOAD_BROKER_FILE)
    await this.acceptAlert()
    let popup = await this.waitForDisplayed(POP_UP)
    await this.click(POP_UP)
    return popup.getText()
  }
  async downloadBrokerDefinitions(filename) {
    return this.click(DOWNLOAD_DEFINITIONS_SECTION)

    /*
    await this.driver.sleep(1000)
    await this.sendKeys(CHOOSE_BROKER_DOWNLOAD_FILE, filename)
    await this.click(DOWNLOAD_BROKER_FILE)
    return driver.sleep(5000);
    */
  }
}
