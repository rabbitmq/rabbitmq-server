const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, idpLoginPage, hasProfile, doUntil } = require('../utils')
const { getManagementUrl, basicAuthorization, deleteUserSessions, getUserSessions } = require('../mgt-api')

const LoginPage = require('../pageobjects/LoginPage')
const SSOHomePage = require('../pageobjects/SSOHomePage')
const OverviewPage = require('../pageobjects/OverviewPage')

const management_username = process.env.MANAGEMENT_USERNAME || 'guest'
const management_password = process.env.MANAGEMENT_PASSWORD || 'guest'

describe('Concurrent Sessions Limits', function () {
  let driver1
  let driver2
  let login1, login2
  let overview1, overview2
  let captureScreen1, captureScreen2
  let isOAuth
  const adminAuth = basicAuthorization('guest', 'guest')

  async function sessionCount() {
    return (await getUserSessions(getManagementUrl(), adminAuth, management_username)).filtered_count
  }

  async function performLogin(driver, loginPage, username, password) {
    if (isOAuth) {
      await loginPage.clickToLogin()
      let idpLogin = idpLoginPage(driver)
      await idpLogin.login(username, password)
    } else {
      await loginPage.login(username, password)
    }
  }

  before(async function () {
    this.timeout(120000)
    if (!hasProfile('sessions')) {
      return this.skip()
    }
    isOAuth = hasProfile('oauth2')
    await deleteUserSessions(getManagementUrl(), adminAuth, management_username)

    driver1 = buildDriver(process.env.RABBITMQ_URL || 'http://localhost:15672/')
    await goToHome(driver1)
    driver2 = buildDriver(process.env.RABBITMQ_URL || 'http://localhost:15672/')
    await goToHome(driver2)

    if (isOAuth) {
      login1 = new SSOHomePage(driver1)
      login2 = new SSOHomePage(driver2)
    } else {
      login1 = new LoginPage(driver1)
      login2 = new LoginPage(driver2)
    }
    overview1 = new OverviewPage(driver1)
    overview2 = new OverviewPage(driver2)
    captureScreen1 = captureScreensFor(driver1, __filename + '_driver1')
    captureScreen2 = captureScreensFor(driver2, __filename + '_driver2')
  })

  it('should allow first login and block second login when limit is 1', async function () {
    await performLogin(driver1, login1, management_username, management_password)
    if (!await overview1.isLoaded(20000)) {
      throw new Error('Failed to login on driver 1')
    }

    await performLogin(driver2, login2, management_username, management_password)
    assert.ok(await login2.isWarningVisible(20000), 'Warning message should be visible on driver 2')
    let warningText = await login2.getWarning()
    assert.ok(warningText.includes('Concurrent session limit reached'), 'Should show limit reached message')
    assert.equal(await sessionCount(), 1)
  })

  it('should free the slot when the first session logs out', async function () {
    await overview1.logout()
    await login1.isLoaded(20000)
    await doUntil(sessionCount, (count) => count == 0, 1000, 'The session was not deleted on logout', 10)

    await goToHome(driver2)
    await performLogin(driver2, login2, management_username, management_password)
    if (!await overview2.isLoaded(20000)) {
      throw new Error('Failed to login on driver 2 after driver 1 logged out')
    }
    assert.equal(await sessionCount(), 1)
  })

  it('should allow the same browser to log in again after logging out', async function () {
    await overview2.logout()
    await login2.isLoaded(20000)
    await doUntil(sessionCount, (count) => count == 0, 1000, 'The session was not deleted on logout', 10)

    await goToHome(driver2)
    await performLogin(driver2, login2, management_username, management_password)
    if (!await overview2.isLoaded(20000)) {
      throw new Error('Failed to login again on driver 2 after logging out')
    }
    assert.equal(await sessionCount(), 1)
  })

  after(async function () {
    if (driver1) await teardown(driver1, this, captureScreen1)
    if (driver2) await teardown(driver2, this, captureScreen2)
  })
})
