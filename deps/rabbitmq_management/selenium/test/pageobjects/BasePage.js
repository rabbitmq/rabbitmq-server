const { By, Key, until, Builder } = require('selenium-webdriver')

const MENU_TABS = By.css('div#menu ul#tabs')
const USER = By.css('li#logout')
const LOGOUT_FORM = By.css('li#logout form')

const CONNECTION_TAB = By.css('div#menu ul#tabs li a[href="#/connections"]')
const CHANNELS_TAB = By.css('div#menu ul#tabs li a[href="#/channels"]')
const QUEUES_TAB = By.css('div#menu ul#tabs li a[href="#/queues"]')
const EXCHANGES_TAB = By.css('div#menu ul#tabs li a[href="#/exchanges"]')
const ADMIN_TAB = By.css('div#menu ul#tabs li a[href="#/users"]')

module.exports = class BasePage {
  driver
  timeout
  polling

  constructor (webdriver) {
    this.driver = webdriver
    // this is another timeout (--timeout 10000) which is the maximum test execution time
    this.timeout = parseInt(process.env.TIMEOUT) || 5000 // max time waiting to locate an element. Should be less that test timeout
    this.polling = parseInt(process.env.POLLING) || 1000 // how frequent selenium searches for an element
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

  async clickOnConnectionsTab () {
    return this.click(CONNECTION_TAB)
  }

  async clickOnAdminTab () {
    return this.click(ADMIN_TAB)
  }

  async clickOnChannelsTab () {
    return this.click(CHANNELS_TAB)
  }

  async clickOnExchangesTab () {
    return this.click(EXCHANGES_TAB)
  }

  async clickOnQueuesTab () {
    return this.click(QUEUES_TAB)
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
    return this.driver.wait(until.elementLocated(locator), this.timeout, 'Timed out after 30 seconds', this.polling);
  }

  async waitForVisible (element) {
    return this.driver.wait(until.elementIsVisible(element), this.timeout, 'Timed out after 30 seconds', this.polling);
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
    return element.click()
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

  capture () {
    this.driver.takeScreenshot().then(
      function (image) {
        require('fs').writeFileSync('/tmp/capture.png', image, 'base64')
      }
    )
  }
}
