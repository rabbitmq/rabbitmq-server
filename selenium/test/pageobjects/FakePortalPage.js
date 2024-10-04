const { By, Key, until, Builder } = require('selenium-webdriver')

const BasePage = require('./BasePage')

const FORM = By.css('form#login_form')
const FAKE_PORTAL_URL = process.env.FAKE_PORTAL_URL || 'http://localhost:3000'

module.exports = class FakePortalPage extends BasePage {
  async isLoaded () {
    return this.waitForDisplayed(FORM)
  }

  async goToHome(client_id = undefined, client_secret = undefined) {
    const url = new URL(FAKE_PORTAL_URL);
    if (typeof client_id !== 'undefined') url.searchParams.append("client_id", client_id);
    if (typeof client_secret !== 'undefined') url.searchParams.append("client_secret", client_secret);
    return this.driver.get(url.href);
  }

  async login () {
    await this.isLoaded()
    return this.submit(FORM)
  }
}
