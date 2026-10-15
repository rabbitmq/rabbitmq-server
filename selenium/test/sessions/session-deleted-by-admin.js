const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, idpLoginPage, hasProfile, doUntil } = require('../utils')
const { getManagementUrl, basicAuthorization, deleteUserSessions, getUserSessions } = require('../mgt-api')

const LoginPage = require('../pageobjects/LoginPage')
const SSOHomePage = require('../pageobjects/SSOHomePage')
const OverviewPage = require('../pageobjects/OverviewPage')

const management_username = process.env.MANAGEMENT_USERNAME || 'guest'
const management_password = process.env.MANAGEMENT_PASSWORD || 'guest'

describe('When an administrator deletes the session of a logged in user', function () {
  let driver
  let login
  let overview
  let captureScreen
  let isOAuth
  const adminAuth = basicAuthorization('guest', 'guest')
  this.timeout(180000)

  async function sessionCount() {
    return (await getUserSessions(getManagementUrl(), adminAuth, management_username)).filtered_count
  }

  async function performLogin() {
    if (isOAuth) {
      await login.clickToLogin()
      let idpLogin = idpLoginPage(driver)
      await idpLogin.login(management_username, management_password)
    } else {
      await login.login(management_username, management_password)
    }
  }

  before(async function () {
    if (!hasProfile('sessions')) {
      return this.skip()
    }
    isOAuth = hasProfile('oauth2')
    await deleteUserSessions(getManagementUrl(), adminAuth, management_username)

    driver = buildDriver()
    await goToHome(driver)
    login = isOAuth ? new SSOHomePage(driver) : new LoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('it is sent back to the login page and can log in again', async function () {
    await performLogin()
    assert.ok(await overview.isLoaded(20000), 'Failed to login')
    assert.equal(await sessionCount(), 1)

    await deleteUserSessions(getManagementUrl(), adminAuth, management_username)
    assert.equal(await sessionCount(), 0)

    // the next heartbeat is rejected by the server
    assert.ok(await login.isLoaded(60000), 'The user should be sent back to the login page')

    await goToHome(driver)
    await performLogin()
    assert.ok(await overview.isLoaded(20000), 'Failed to login again')
    assert.equal(await sessionCount(), 1)
  })

  after(async function () {
    if (driver) await teardown(driver, this, captureScreen)
  })
})
