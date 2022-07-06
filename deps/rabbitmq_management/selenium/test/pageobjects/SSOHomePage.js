const {By,Key,until,Builder} = require("selenium-webdriver");

var BasePage = require('./BasePage')

const LOGIN_BUTTON = By.css('div#outer div#login div#login-status button#loginWindow');
const WARNING = By.css('.warning')

module.exports = class SSOLoginPage extends BasePage {

  async isLoaded () {
    return this.waitForDisplayed(LOGIN_BUTTON)
  }
  async clickToLogin(username, password) {
    await this.isLoaded();
    if (! await this.isWarningVisible() ) {
      return this.click(LOGIN_BUTTON)
    } else {
      this.capture();
      var message = await this.getWarning()
      throw new Error(`Warning message  "`+ message + `" is visible. Idp is probably down or not reachable`)
    }
  }
  async getLoginButton() {
    return this.getText(LOGIN_BUTTON)
  }
  async isWarningVisible() {
      try {
        await this.getText(WARNING)
        return true;
      }catch(e) {
        return false;
      }
  }
  async getWarning() {
    return this.getText(WARNING)
  }

}
