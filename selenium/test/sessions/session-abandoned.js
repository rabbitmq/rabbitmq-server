const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, idpLoginPage, hasProfile, doUntil } = require('../utils')
const { getManagementUrl, basicAuthorization, deleteUserSessions, getUserSessions } = require('../mgt-api')

const LoginPage = require('../pageobjects/LoginPage')
const SSOHomePage = require('../pageobjects/SSOHomePage')
const OverviewPage = require('../pageobjects/OverviewPage')

const management_username = process.env.MANAGEMENT_USERNAME || 'guest'
const management_password = process.env.MANAGEMENT_PASSWORD || 'guest'

describe('When a logged in user leaves without logging out', function () {
  let driver1, driver2
  let login1, login2
  let overview1, overview2
  let captureScreen1, captureScreen2
  let isOAuth
  const adminAuth = basicAuthorization('guest', 'guest')
  this.timeout(240000)

  async function sessionCount() {
    return (await getUserSessions(getManagementUrl(), adminAuth, management_username)).filtered_count
  }

  async function performLogin(driver, loginPage) {
    if (isOAuth) {
      await loginPage.clickToLogin()
      let idpLogin = idpLoginPage(driver)
      await idpLogin.login(management_username, management_password)
    } else {
      await loginPage.login(management_username, management_password)
    }
  }

  before(async function () {
    if (!hasProfile('sessions')) {
      return this.skip()
    }
    isOAuth = hasProfile('oauth2')
    await deleteUserSessions(getManagementUrl(), adminAuth, management_username)

    driver1 = buildDriver()
    await goToHome(driver1)
    driver2 = buildDriver()
    await goToHome(driver2)
    login1 = isOAuth ? new SSOHomePage(driver1) : new LoginPage(driver1)
    login2 = isOAuth ? new SSOHomePage(driver2) : new LoginPage(driver2)
    overview1 = new OverviewPage(driver1)
    overview2 = new OverviewPage(driver2)
    captureScreen1 = captureScreensFor(driver1, __filename + '_driver1')
    captureScreen2 = captureScreensFor(driver2, __filename + '_driver2')
  })

  it('its session is released once it stops sending heartbeats', async function () {
    await performLogin(driver1, login1)
    assert.ok(await overview1.isLoaded(20000), 'Failed to login on driver 1')

    // leaving the page stops the heartbeats; the session is not deleted
    await driver1.driver.get('about:blank')

    await goToHome(driver2)
    await performLogin(driver2, login2)
    assert.ok(await login2.isWarningVisible(20000), 'The slot should still be taken')

    await doUntil(sessionCount, (count) => count == 0,
      5000, 'The abandoned session was not released', 24)

    await goToHome(driver2)
    await performLogin(driver2, login2)
    assert.ok(await overview2.isLoaded(20000), 'Failed to login after the abandoned session was released')
    assert.equal(await sessionCount(), 1)
  })

  after(async function () {
    if (driver1) await teardown(driver1, this, captureScreen1)
    if (driver2) await teardown(driver2, this, captureScreen2)
  })
})
