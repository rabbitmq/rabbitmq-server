const { By, Key, until, Builder } = require('selenium-webdriver')

const BasePage = require('./BasePage')

const LOGIN = By.css('div#outer div#login')
const LOGOUT_BUTTON = By.css('div#outer div#login-status button#logout')
const OAUTH2_LOGIN_BUTTON = By.css('div#outer div#login button#login')
const SELECT_RESOURCES = By.css('div#outer div#login select#oauth2-resource')
const WARNING = By.css('div#outer div#login div#login-status p.warning')

const SECTION_LOGIN_WITH_OAUTH = By.css('div#outer div#login div#login-with-oauth2')
const SECTION_LOGIN_WITH_BASIC_AUTH = By.css('div#outer div#login div#login-with-basic-auth')
const BASIC_AUTH_LOGIN_BUTTON = By.css('form#basic-auth-form input[type=submit]')

const BASIC_AUTH_LOGIN_FORM = By.css('form#basic-auth-form')
const BASIC_AUTH_LOGIN_USERNAME = By.css('form#basic-auth-form input#username')
const BASIC_AUTH_LOGIN_PASSWORD = By.css('form#basic-auth-form input#password')

module.exports = class SSOHomePage extends BasePage {
  async isLoaded () {
    return this.waitForDisplayed(LOGIN)
  }

  async clickToLogin () {
    await this.isLoaded()
    return this.click(OAUTH2_LOGIN_BUTTON)
  }
  async clickToBasicAuthLogin () {
    await this.isLoaded()
    return this.click(BASIC_AUTH_LOGIN_BUTTON)
  }

  async clickToLogout() {
    await this.isLoaded()
    return this.click(LOGOUT_BUTTON)
  }
  async getLoginButton () {
    return this.getText(OAUTH2_LOGIN_BUTTON)
  }
  async getLogoutButton () {
    return this.getText(LOGOUT_BUTTON)
  }
  async getBasicAuthLoginButton () {
    return this.getValue(BASIC_AUTH_LOGIN_BUTTON)
  }

  async chooseOauthResource(text) {
    return this.selectOption(SELECT_RESOURCES, text)
  }

  async getOAuthResourceOptions () {
    return this.getSelectableOptions(SELECT_RESOURCES)
  }
  async isLoginButtonVisible() {
    try {
      await this.waitForDisplayed(OAUTH2_LOGIN_BUTTON)
      return Promise.resolve(true)
    } catch (e) {
      return Promise.resolve(false)
    }
  }
  async isLogoutButtonVisible() {
    try {
      await this.waitForDisplayed(LOGOUT_BUTTON)
      return Promise.resolve(true)
    } catch (e) {
      return Promise.resolve(false)
    }
  }

  async isOAuth2SectionVisible() {
    return this.isDisplayed(SECTION_LOGIN_WITH_OAUTH)
  }
  async getOAuth2Section() {
    return this.waitForDisplayed(SECTION_LOGIN_WITH_OAUTH)
  }
  async isBasicAuthSectionVisible() {
    return this.isDisplayed(SECTION_LOGIN_WITH_BASIC_AUTH)
  }
  async getBasicAuthSection() {
    return this.waitForDisplayed(SECTION_LOGIN_WITH_BASIC_AUTH)
  }

  async toggleBasicAuthSection() {
    await this.click(SECTION_LOGIN_WITH_BASIC_AUTH)
  }

  async basicAuthLogin (username, password) {
    await this.isLoaded()
    await this.sendKeys(BASIC_AUTH_LOGIN_USERNAME, username)
    await this.sendKeys(BASIC_AUTH_LOGIN_PASSWORD, password)
    return this.submit(BASIC_AUTH_LOGIN_FORM)
  }


  async isWarningVisible () {
    try {
      await this.waitForDisplayed(WARNING)
      return Promise.resolve(true)
    } catch (e) {
      return Promise.resolve(false)
    }
  }
  async getWarnings() {
    try
    {
      return driver.findElements(WARNING)
    } catch (NoSuchElement) {
      return Promise.resolve([])
    }
  }
  async getWarning () {
    return this.getText(WARNING)
  }
}
