const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, idpLoginPage, hasProfile } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const SSOHomePage = require('../pageobjects/SSOHomePage')
const OverviewPage = require('../pageobjects/OverviewPage')

const management_username = process.env.MANAGEMENT_USERNAME || 'guest'
const management_password = process.env.MANAGEMENT_PASSWORD || 'guest'
const rabbitmq_url_1 = process.env.RABBITMQ_URL_1 || process.env.RABBITMQ_URL || 'http://localhost:15672/'
const rabbitmq_url_2 = process.env.RABBITMQ_URL_2 || process.env.RABBITMQ_URL || 'http://localhost:15672/'

describe('Distributed Conflict Resolution', function () {
  let driver1
  let driver2
  let login1, login2
  let overview1, overview2
  let captureScreen1, captureScreen2
  let isOAuth

  before(async function () { 
    this.timeout(120000)
    isOAuth = hasProfile('oauth2')

    // Initialize two separate browser instances
    // Initialize two separate browser instances sequentially to avoid overwhelming Selenium Server
    // driver1 connects to the first node
    driver1 = buildDriver(rabbitmq_url_1)
    await driver1.driver.sleep(2000)
    console.log('Driver 1 connecting to:', driver1.baseUrl)
    await goToHome(driver1)
    
    // driver2 connects to the second node
    driver2 = buildDriver(rabbitmq_url_2)
    await driver2.driver.sleep(2000)
    console.log('Driver 2 connecting to:', driver2.baseUrl)
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

  beforeEach(async function () {
    // Clear sessions for the test user to ensure a clean slate before the test
    try {
      const { getManagementUrl, basicAuthorization, deleteUserSessions } = require('../mgt-api')
      const adminAuth = basicAuthorization('guest', 'guest') // Assuming guest is admin
      await deleteUserSessions(getManagementUrl(), adminAuth, management_username)
      await new Promise(r => setTimeout(r, 1000)) // Give cluster gossip time to propagate
    } catch (e) {
      console.error("Failed to clean up sessions before test:", e)
    }
  })

  async function performLogin(driver, loginPage, username, password) {
    if (isOAuth) {
      await loginPage.clickToLogin()
      let idpLogin = idpLoginPage(driver)
      await idpLogin.login(username, password)
    } else {
      await loginPage.login(username, password)
    }
  }

  it('should enforce limits across the cluster with simultaneous logins', async function () {
    // Attempt login to Node 1 and Node 2 simultaneously
    await Promise.all([
      performLogin(driver1, login1, management_username, management_password),
      performLogin(driver2, login2, management_username, management_password)
    ])

    // Wait until one succeeds and the other fails, up to 30 seconds
    await driver1.driver.wait(async () => {
      let isLoaded1 = await overview1.isLoaded(5000, true).catch(() => false)
      let isLoaded2 = await overview2.isLoaded(5000, true).catch(() => false)
      let isWarningVisible1 = await login1.isWarningVisible(5000, true).catch(() => false)
      let isWarningVisible2 = await login2.isWarningVisible(5000, true).catch(() => false)

      return (isLoaded1 && isWarningVisible2) || (isLoaded2 && isWarningVisible1)
    }, 30000, 'One login should succeed and the other should fail with a warning')

    // Double check the state to assert
    let isLoaded1 = await overview1.isLoaded(5000, true).catch(() => false)
    let isLoaded2 = await overview2.isLoaded(5000, true).catch(() => false)

    let isWarningVisible1 = await login1.isWarningVisible(5000, true).catch(() => false)
    let isWarningVisible2 = await login2.isWarningVisible(5000, true).catch(() => false)

    // Exactly one should succeed, exactly one should fail
    assert.ok(isLoaded1 || isLoaded2, 'At least one login should succeed')
    assert.ok(!(isLoaded1 && isLoaded2), 'Both logins should not succeed')

    assert.ok(isWarningVisible1 || isWarningVisible2, 'At least one warning should be visible')
    
    if (isWarningVisible1) {
      let warningText1 = await login1.getWarning()
      assert.ok(warningText1.includes('Concurrent session limit reached'), 'Should show limit reached message on Node 1')
    }
    if (isWarningVisible2) {
      let warningText2 = await login2.getWarning()
      assert.ok(warningText2.includes('Concurrent session limit reached'), 'Should show limit reached message on Node 2')
    }
  })

  after(async function () {
    if (driver1) await teardown(driver1, this, captureScreen1)
    if (driver2) await teardown(driver2, this, captureScreen2)
  })
})
