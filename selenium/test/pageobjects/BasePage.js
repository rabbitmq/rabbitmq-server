const { By, Key, until, Builder, Select, WebDriverError,  NoSuchSessionError } = require('selenium-webdriver')

const MENU_TABS = By.css('div#menu ul#tabs')
const USER = By.css('li#logout')
const LOGOUT_FORM = By.css('li#logout form')
const SELECT_VHOSTS = By.css('select#show-vhost')
const SELECT_REFRESH = By.css('ul#topnav li#interval select#update-every')
const OVERVIEW_TAB = By.css('div#menu ul#tabs li#overview')
const CONNECTIONS_TAB = By.css('div#menu ul#tabs li#connections')
const CHANNELS_TAB = By.css('div#menu ul#tabs li#channels')
const QUEUES_AND_STREAMS_TAB = By.css('div#menu ul#tabs li#queues-and-streams')
const EXCHANGES_TAB = By.css('div#menu ul#tabs li#exchanges')
const ADMIN_TAB = By.css('div#menu ul#tabs li#admin')
const STREAM_CONNECTIONS_TAB = By.css('div#menu ul#tabs li#stream-connections')

const FORM_POPUP = By.css('div.form-popup-warn')
const FORM_POPUP_CLOSE_BUTTON = By.css('div.form-popup-warn span')

module.exports = class BasePage {
  driver
  timeout
  polling
  interactionDelay

  constructor (webdriver) {
    this.driver = webdriver
    this.timeout = parseInt(process.env.SELENIUM_TIMEOUT) || 1000 // max time waiting to locate an element. Should be less that test timeout
    this.polling = parseInt(process.env.SELENIUM_POLLING) || 500 // how frequent selenium searches for an element
    this.interactionDelay = parseInt(process.env.SELENIUM_INTERACTION_DELAY) || 0 // slow down interactions (when rabbit is behind a http proxy)
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
  async selectRefreshOption(option) {
    return this.selectOption(SELECT_REFRESH, option)
  }
  async waitForOverviewTab() {
    await this.driver.sleep(250)
    return this.waitForDisplayed(OVERVIEW_TAB)
  }

  async clickOnOverviewTab () {
    return this.click(CONNECTIONS_TAB)
  }

  async clickOnConnectionsTab () {
    return this.click(CONNECTIONS_TAB)
  }
  async waitForConnectionsTab() {
    await this.driver.sleep(250)
    return this.waitForDisplayed(CONNECTIONS_TAB)
  }

  async clickOnAdminTab () {
    return this.click(ADMIN_TAB)
  }
  async waitForAdminTab() {
    await this.driver.sleep(250)
    return this.waitForDisplayed(ADMIN_TAB)
  }

  async clickOnChannelsTab () {
    return this.click(CHANNELS_TAB)
  }
  async waitForChannelsTab() {
    await this.driver.sleep(250)
    return this.waitForDisplayed(CHANNELS_TAB)
  }

  async clickOnExchangesTab () {
    return this.click(EXCHANGES_TAB)
  }
  async waitForExchangesTab() {
    await this.driver.sleep(250)
    return this.waitForDisplayed(EXCHANGES_TAB)
  }

  async clickOnQueuesTab () {
    return this.click(QUEUES_AND_STREAMS_TAB)
  }
  async waitForQueuesTab() {
    await this.driver.sleep(250)
    return this.waitForDisplayed(QUEUES_AND_STREAMS_TAB)
  }

  async clickOnStreamTab () {
    return this.click(STREAM_CONNECTIONS_TAB)
  }
  async waitForStreamConnectionsTab() {
    return this.waitForDisplayed(STREAM_CONNECTIONS_TAB)
  }

  async getSelectableOptions(locator) {
    let selectable = await this.waitForDisplayed(locator)
    const select = await new Select(selectable)
    const optionList = await select.getOptions()

    let table_model = []
    for (const index in optionList) {
      const t = await optionList[index].getText()
      const v = await optionList[index].getAttribute('value')
      table_model.push({"text":t, "value": v})
    }

    return table_model
  }
  async selectOption(locator, text) {
    let selectable = await this.waitForDisplayed(locator)
    const select = await new Select(selectable)
    return select.selectByVisibleText(text)
  }

  async getSelectableVhosts() {
    const table_model = await this.getSelectableOptions(SELECT_VHOSTS)
    let new_table_model = []
    for (let i = 0; i < table_model.length; i++) {
      new_table_model.push(await table_model[i].text)
    }
    return new_table_model
  }


  async getTable(tableLocator, firstNColumns, rowClass) {
    const table = await this.waitForDisplayed(tableLocator)
    const rows = await table.findElements(rowClass == undefined ? 
        By.css('tbody tr') : By.css('tbody tr.' + rowClass))
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
  async isPopupWarningDisplayed() {
    try  {      
      let element = await driver.findElement(FORM_POPUP)
      return element.isDisplayed()
    } catch(e) {
      return Promise.resolve(false)
    }
    /*
    let element = await driver.findElement(FORM_POPUP)
    return this.driver.wait(until.elementIsVisible(element), this.timeout / 2,
      'Timed out after [timeout=' + this.timeout + ';polling=' + this.polling + '] awaiting till visible ' + element,
      this.polling / 2).then(function onWarningVisible(e) {
          return Promise.resolve(true)
      }, function onError(e) {
          return Promise.resolve(false)
      })
      */
  }
  async getPopupWarning() {
    let element = await driver.findElement(FORM_POPUP)
    return this.driver.wait(until.elementIsVisible(element), this.timeout,
      'Timed out after [timeout=' + this.timeout + ';polling=' + this.polling + '] awaiting till visible ' + element,
      this.polling).getText().then((value) => value.substring(0, value.search('\n\nClose')))
  }
  async closePopupWarning() {
    return this.click(FORM_POPUP_CLOSE_BUTTON)
  }
  async isDisplayed(locator) {
      try {
        let element = await driver.findElement(locator)

        return this.driver.wait(until.elementIsVisible(element), this.timeout,
          'Timed out after [timeout=' + this.timeout + ';polling=' + this.polling + '] awaiting till visible ' + element,
          this.polling / 2)
      }catch(error) {
          return Promise.resolve(false)
      }
  }

  async waitForLocated (locator) {
    let attempts = 3
    let retry = false
    let rethrowError = null
    do {
      try {
        return this.driver.wait(until.elementLocated(locator), this.timeout,
          'Timed out after [timeout=' + this.timeout + ';polling=' + this.polling + '] seconds locating ' + locator,
          this.polling)
      }catch(error) {
        if (error.name.includes("StaleElementReferenceError")) {
          retry = true
        }else if (!error.name.includes("NoSuchSessionError")) {
          console.error("Failed waitForLocated " + locator + " due to " + error)
          retry = false
        }
        rethrowError = error
      }  
    } while (retry && --attempts > 0)
    throw rethrowError
  }

  async waitForVisible (element) {
    let attempts = 3
    let retry = false
    let rethrowError = null
    do {      
      try {
        return this.driver.wait(until.elementIsVisible(element), this.timeout,
          'Timed out after [timeout=' + this.timeout + ';polling=' + this.polling + '] awaiting till visible ' + element,
          this.polling)
      }catch(error) {         
        if (error.name.includes("StaleElementReferenceError")) {
          retry = true
        }else if (!error.name.includes("NoSuchSessionError")) {
          console.error("Failed to find visible element " + element + " due to " + error)
          retry = false
        }        
        rethrowError = error
      }
    } while (retry && --attempts > 0)
    throw rethrowError
  }


  async waitForDisplayed (locator) {
    let attempts = 3
    let retry = false
    let rethrowError = null
    do {
      if (this.interactionDelay && this.interactionDelay > 0) await this.driver.sleep(this.interactionDelay)
        try {
          return this.waitForVisible(await this.waitForLocated(locator))
        }catch(error) {
          if (error.name.includes("StaleElementReferenceError")) {
            retry = true
          }else if (!error.name.includes("NoSuchSessionError")) {
            retry = false
            console.error("Failed to waitForDisplayed " + locator + " due to " + error)
          }
          rethrowError = error
        }
    } while (retry && --attempts > 0 )
    throw rethrowError
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
    if (this.interactionDelay) await this.driver.sleep(this.interactionDelay)

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
    const remote = require('selenium-webdriver/remote');
    driver.setFileDetector(new remote.FileDetector);
    return element.sendKeys(file)
  }
  async acceptAlert () {
    await this.driver.wait(until.alertIsPresent(), this.timeout);
    await this.driver.sleep(250)
    const alert = await this.driver.switchTo().alert();
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
