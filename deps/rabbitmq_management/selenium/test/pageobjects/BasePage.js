const { By, Key, until, Builder } = require('selenium-webdriver')

const MENU_TABS = By.css('div#menu ul#tabs')
const USER = By.css('li#logout')
const LOGOUT_FORM = By.css('li#logout form')
const SELECT_VHOSTS = By.css('select#show-vhost')

const OVERVIEW_TAB = By.css('div#menu ul#tabs li#overview')
const CONNECTIONS_TAB = By.css('div#menu ul#tabs li#connections')
const CHANNELS_TAB = By.css('div#menu ul#tabs li#channels')
const QUEUES_AND_STREAMS_TAB = By.css('div#menu ul#tabs li#queues-and-streams')
const EXCHANGES_TAB = By.css('div#menu ul#tabs li#exchanges')
const ADMIN_TAB = By.css('div#menu ul#tabs li#admin')
const STREAM_CONNECTIONS_TAB = By.css('div#menu ul#tabs li#stream-connections')

module.exports = class BasePage {
  driver
  timeout
  polling

  constructor (webdriver) {
    this.driver = webdriver
    // this is another timeout (--timeout 10000) which is the maximum test execution time
    this.timeout = parseInt(process.env.TIMEOUT) || 5000 // max time waiting to locate an element. Should be less that test timeout
    this.polling = parseInt(process.env.POLLING) || 500 // how frequent selenium searches for an element
  }

  async isLoaded () {
    return this.waitForDisplayed(MENU_TABS)
  }

  async logout () {
    await this.submit(LOGOUT_FORM)
  }

  async getUser () {
    return this.getText(USER)
  }

  async waitForOverviewTab() {
    return this.waitForDisplayed(OVERVIEW_TAB)
  }

  async clickOnOverviewTab () {
    return this.click(CONNECTIONS_TAB)
  }

  async clickOnConnectionsTab () {
    return this.click(CONNECTIONS_TAB)
  }
  async waitForConnectionsTab() {
    return this.waitForDisplayed(CONNECTIONS_TAB)
  }

  async clickOnAdminTab () {
    return this.click(ADMIN_TAB)
  }
  async waitForAdminTab() {
    return this.waitForDisplayed(ADMIN_TAB)
  }

  async clickOnChannelsTab () {
    return this.click(CHANNELS_TAB)
  }
  async waitForChannelsTab() {
    return this.waitForDisplayed(CHANNELS_TAB)
  }

  async clickOnExchangesTab () {
    return this.click(EXCHANGES_TAB)
  }
  async waitForExchangesTab() {
    return this.waitForDisplayed(EXCHANGES_TAB)
  }

  async clickOnQueuesTab () {
    return this.click(QUEUES_AND_STREAMS_TAB)
  }
  async waitForQueuesTab() {
    return this.waitForDisplayed(QUEUES_AND_STREAMS_TAB)
  }

  async clickOnStreamTab () {
    return this.click(STREAM_CONNECTIONS_TAB)
  }
  async waitForStreamConnectionsTab() {
    return this.waitForDisplayed(STREAM_CONNECTIONS_TAB)
  }

  async getSelectableVhosts() {
    let selectable = await this.waitForDisplayed(SELECT_VHOSTS)
    let options = await selectable.findElements(By.css('option'))
    let table_model = []
    for (let option of options) {
      table_model.push(await option.getText())
    }
    return table_model
  }

  async getTable(locator, firstNColumns) {
    let table = await this.waitForDisplayed(locator)
    let rows = await table.findElements(By.css('tbody tr'))
    let table_model = []
    for (let row of rows) {
      let columns = await row.findElements(By.css('td'))
      let table_row = []
      for (let column of columns) {
        if (table_row.length < firstNColumns) table_row.push(await column.getText())
      }
      table_model.push(table_row)
    }
    return table_model
  }

  async waitForLocated (locator) {
    try {
      return this.driver.wait(until.elementLocated(locator), this.timeout,
        'Timed out after 30 seconds locating ' + locator, this.polling)
    }catch(error) {
      console.error("Failed to locate element " + locator)
      throw error
    }
  }

  async waitForVisible (element) {
    try {
      return this.driver.wait(until.elementIsVisible(element), this.timeout,
        'Timed out after 30 seconds awaiting till visible ' + element, this.polling)
    }catch(error) {
      console.error("Failed to find visible element " + element)
      throw error
    }
  }

  async waitForDisplayed (locator) {
    return this.waitForVisible(await this.waitForLocated(locator))
  }

  async getText (locator) {
    const element = await this.waitForDisplayed(locator)
    return element.getText()
  }

  async getValue (locator) {
    const element = await this.waitForDisplayed(locator)
    return element.getAttribute('value')
  }

  async click (locator) {
    const element = await this.waitForDisplayed(locator)
    try {
      return element.click()
    } catch(error) {
      console.error("Failed to click on " + locator + " due to " + error);
      throw error;
    }
  }

  async submit (locator) {
    const element = await this.waitForDisplayed(locator)
    return element.submit()
  }

  async sendKeys (locator, keys) {
    const element = await this.waitForDisplayed(locator)
    await element.click()
    await element.clear()
    return element.sendKeys(keys)
  }

  async chooseFile (locator, file) {
    const element = await this.waitForDisplayed(locator)
    var remote = require('selenium-webdriver/remote');
    driver.setFileDetector(new remote.FileDetector);
    return element.sendKeys(file)
  }
  async acceptAlert () {
    await this.driver.wait(until.alertIsPresent(), this.timeout);
    await this.driver.sleep(250)
    let alert = await this.driver.switchTo().alert();
    await this.driver.sleep(250)
    return alert.accept();
  }
  log(message) {
    console.log(new Date() + " " + message)
  }

  capture () {
    this.driver.takeScreenshot().then(
      function (image) {
        require('fs').writeFileSync('/tmp/capture.png', image, 'base64')
      }
    )
  }
}
