const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, hasProfile, doUntil, delay } = require('../utils')
const { getManagementUrl, basicAuthorization, deleteUserSessions, getUserSessions } = require('../mgt-api')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')

describe('Once the login session of a user expires', function () {
  let driver
  let login
  let overview
  let captureScreen
  const adminAuth = basicAuthorization('guest', 'guest')
  this.timeout(240000)

  async function sessionCount() {
    return (await getUserSessions(getManagementUrl(), adminAuth, 'guest')).filtered_count
  }

  before(async function () {
    if (!hasProfile('one-minute-login-timeout')) {
      return this.skip()
    }
    await deleteUserSessions(getManagementUrl(), adminAuth, 'guest')

    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('its session is deleted and it can log in again', async function () {
    await login.login('guest', 'guest')
    assert.ok(await overview.isLoaded(20000), 'Failed to login')
    assert.equal(await sessionCount(), 1)

    await delay(60000)

    assert.ok(await login.isLoaded(60000), 'The user should be sent back to the login page')
    await doUntil(sessionCount, (count) => count == 0,
      5000, 'The session of the expired login was not deleted', 12)

    await login.login('guest', 'guest')
    assert.ok(await overview.isLoaded(20000), 'Failed to login again')
    assert.equal(await sessionCount(), 1)
  })

  after(async function () {
    if (driver) await teardown(driver, this, captureScreen)
  })
})
