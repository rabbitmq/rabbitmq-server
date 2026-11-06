const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, idpLoginPage, hasProfile, doUntil } = require('../utils')
const { getManagementUrl, basicAuthorization, deleteUserSessions, getUserSessions } = require('../mgt-api')

const LoginPage = require('../pageobjects/LoginPage')
const SSOHomePage = require('../pageobjects/SSOHomePage')
const OverviewPage = require('../pageobjects/OverviewPage')

const management_username = process.env.MANAGEMENT_USERNAME || 'guest'
const management_password = process.env.MANAGEMENT_PASSWORD || 'guest'

describe('Concurrent Sessions Limits of 2', function () {
  let drivers = []
  let logins = []
  let overviews = []
  let captureScreens = []
  let isOAuth
  const adminAuth = basicAuthorization('guest', 'guest')

  async function sessionCount() {
    return (await getUserSessions(getManagementUrl(), adminAuth, management_username)).filtered_count
  }

  async function performLogin(i) {
    if (isOAuth) {
      await logins[i].clickToLogin()
      let idpLogin = idpLoginPage(drivers[i])
      await idpLogin.login(management_username, management_password)
    } else {
      await logins[i].login(management_username, management_password)
    }
  }

  before(async function () {
    this.timeout(180000)
    if (!hasProfile('two-logins-limit')) {
      return this.skip()
    }
    isOAuth = hasProfile('oauth2')
    await deleteUserSessions(getManagementUrl(), adminAuth, management_username)

    for (let i = 0; i < 3; i++) {
      let driver = buildDriver(process.env.RABBITMQ_URL || 'http://localhost:15672/')
      drivers.push(driver)
      await goToHome(driver)
      logins.push(isOAuth ? new SSOHomePage(driver) : new LoginPage(driver))
      overviews.push(new OverviewPage(driver))
      captureScreens.push(captureScreensFor(driver, __filename + '_driver' + (i + 1)))
    }
  })

  it('should allow two logins and block the third', async function () {
    await performLogin(0)
    assert.ok(await overviews[0].isLoaded(20000), 'Failed to login on driver 1')
    await performLogin(1)
    assert.ok(await overviews[1].isLoaded(20000), 'Failed to login on driver 2')

    await performLogin(2)
    assert.ok(await logins[2].isWarningVisible(20000), 'Warning message should be visible on driver 3')
    let warningText = await logins[2].getWarning()
    assert.ok(warningText.includes('Concurrent session limit reached'), 'Should show limit reached message')
    assert.equal(await sessionCount(), 2)
  })

  it('should free exactly one slot when one session logs out', async function () {
    await overviews[0].logout()
    await logins[0].isLoaded(20000)
    await doUntil(sessionCount, (count) => count == 1, 1000, 'Only one session should be left after the logout', 10)

    await goToHome(drivers[2])
    await performLogin(2)
    assert.ok(await overviews[2].isLoaded(20000), 'Failed to login on driver 3 after driver 1 logged out')
    assert.ok(await overviews[1].isLoaded(5000), 'Driver 2 should still be logged in')
    assert.equal(await sessionCount(), 2)
  })

  after(async function () {
    for (let i = 0; i < drivers.length; i++) {
      await teardown(drivers[i], this, captureScreens[i])
    }
  })
})
