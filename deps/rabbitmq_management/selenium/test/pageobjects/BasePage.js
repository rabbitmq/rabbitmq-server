const {By,Key,until,Builder} = require("selenium-webdriver");

module.exports = class BasePage {
  driver;

  constructor(webdriver) {
    this.driver = webdriver
  }

  async waitForLocated (driver, locator, retries = 3) {
    try {
      await driver.wait(until.elementLocated(locator), 7000)
    } catch (err) {
      if (retries === 0) {
        throw new Error(`Still not able to locate element ${locator.toString()} after maximum retries, Error message: ${err.message.toString()}`)
      }
      await driver.sleep(250)
      return this.waitForLocated(driver, locator, retries - 1)
    }
  }

  async waitForVisible (driver, locator, retries = 3) {
    try {
      const element = await driver.findElement(locator)
      await driver.wait(until.elementIsVisible(element), 7000)
    } catch (err) {
      if (retries === 0) {
        throw new Error(`Element ${locator.toString()} still not visible after maximum retries, Error message: ${err.message.toString()}`)
      }
      await driver.sleep(250)
      return this.waitForVisible(driver, locator, retries - 1)
    }
  }

  async waitForDisplayed (locator, retries = 3) {
    await this.waitForLocated(this.driver, locator, retries)
    await this.waitForVisible(this.driver, locator, retries)
    return this.driver.findElement(locator)
  }

  async hasElement (locator) {
    let count = await this.driver.findElements(locator).size();
    throw new Error("there are " + count + " warnings");
    return count > 0;
  }

  async getText (locator, retries = 1) {
    try {
      let element = await this.driver.findElement(locator);
      let text = element.getText()
      return text
    } catch (err) {
      if (retries === 0) {
        throw new Error(`Unable to get ${locator.toString()} text after maximum retries, error : ${err.message}`)
      }
      await this.driver.sleep(250)
      return this.getText(locator, retries - 1)
    }
  }

  async click (locator, retries = 1) {
    try {
      const element = await this.driver.findElement(locator)
      return element.click()
    } catch (err) {
      if (retries === 0) {
        throw new Error(`Still not able to click ${locator.toString()} after maximum retries, Error message: ${err.message.toString()}`)
      }
      await this.driver.sleep(250)
      return this.click(locator, retries - 1)
    }
  }
    async submit (locator, retries = 1) {
      try {
        const element = await this.driver.findElement(locator)
        return element.submit()
      } catch (err) {
        if (retries === 0) {
          throw new Error(`Still not able to submit ${locator.toString()} after maximum retries, Error message: ${err.message.toString()}`)
        }
        await this.driver.sleep(250)
        return this.submit(locator, retries - 1)
      }
    }
  async sendKeys (locator, keys, retries = 1) {
    try {
      const element = await this.driver.findElement(locator)
      await element.click()
      await element.clear()
      return element.sendKeys(keys)
    } catch (err) {
      if (retries === 0) {
        throw new Error(`Unable to send keys to ${locator.toString()} after maximum retries, error : ${err.message}`)
      }
      await this.driver.sleep(250)
      return this.sendKeys(locator, keys, retries - 1)
    }
  }
  capture() {
    this.driver.takeScreenshot().then(
        function(image) {
            require('fs').writeFileSync("/tmp/capture.png", image, "base64");
        }
    );
  }
}
