const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, idpLoginPage, hasProfile, doUntil } = require('../utils')
const { getManagementUrl, basicAuthorization, deleteUserSessions, getUserSessions } = require('../mgt-api')

const SSOHomePage = require('../pageobjects/SSOHomePage')
const OverviewPage = require('../pageobjects/OverviewPage')

const management_username = process.env.MANAGEMENT_USERNAME || 'guest'
const management_password = process.env.MANAGEMENT_PASSWORD || 'guest'

describe('When the OAuth 2 token of a logged in user is no longer valid', function () {
  let driver
  let login
  let overview
  let captureScreen
  const adminAuth = basicAuthorization('guest', 'guest')
  this.timeout(240000)

  async function sessionCount() {
    return (await getUserSessions(getManagementUrl(), adminAuth, management_username)).filtered_count
  }

  async function performLogin() {
    await login.clickToLogin()
    let idpLogin = idpLoginPage(driver)
    await idpLogin.login(management_username, management_password)
  }

  before(async function () {
    if (!hasProfile('sessions') || !hasProfile('oauth2')) {
      return this.skip()
    }
    await deleteUserSessions(getManagementUrl(), adminAuth, management_username)

    driver = buildDriver()
    await goToHome(driver)
    login = new SSOHomePage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('its session is released and it can log in again', async function () {
    await performLogin()
    assert.ok(await overview.isLoaded(20000), 'Failed to login')
    assert.equal(await sessionCount(), 1)

    await driver.driver.executeScript("set_token_auth('invalid-token')")

    // the next heartbeat is rejected; the session can no longer be refreshed
    assert.ok(await login.isLoaded(60000), 'The user should be sent back to the login page')
    await doUntil(sessionCount, (count) => count == 0,
      5000, 'The session was not released after the token became invalid', 24)

    await goToHome(driver)
    await performLogin()
    assert.ok(await overview.isLoaded(20000), 'Failed to login again')
    assert.equal(await sessionCount(), 1)
  })

  after(async function () {
    if (driver) await teardown(driver, this, captureScreen)
  })
})
