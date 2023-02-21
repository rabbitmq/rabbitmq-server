const { By, Key, until, Builder } = require('selenium-webdriver')

const BasePage = require('./BasePage')

const LOGIN_BUTTON = By.css('div#outer div#login div#login-status button#loginWindow')
const WARNING = By.css('p.warning')

module.exports = class SSOHomePage extends BasePage {
  async isLoaded () {
    return this.waitForDisplayed(LOGIN_BUTTON)
  }

  async clickToLogin () {
    await this.isLoaded()
    if (!await this.isWarningVisible()) {
      return this.click(LOGIN_BUTTON)
    } else {
      this.capture()
      const message = await this.getWarning()
      throw new Error('Warning message  "' + message + '" is visible. Idp is probably down or not reachable')
    }
  }

  async getLoginButton () {
    return this.getText(LOGIN_BUTTON)
  }

  async isWarningVisible () {
    try {
      await this.getText(WARNING)
      return Promise.resolve(true)
    } catch (e) {
      return Promise.resolve(false)
    }
  }

  async getWarning () {
    return this.getText(WARNING)
  }
}
