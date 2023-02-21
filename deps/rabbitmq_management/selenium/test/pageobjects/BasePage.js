const { By, Key, until, Builder } = require('selenium-webdriver')

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
