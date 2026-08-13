const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, idpLoginPage, hasProfile } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const SSOHomePage = require('../pageobjects/SSOHomePage')
const OverviewPage = require('../pageobjects/OverviewPage')

const management_username = process.env.MANAGEMENT_USERNAME || 'guest'
const management_password = process.env.MANAGEMENT_PASSWORD || 'guest'
const other_rabbitmq_url = process.env.OTHER_RABBITMQ_URL

describe('Distributed Conflict Resolution', function () {
  let driver1
  let driver2
  let login1, login2
  let overview1, overview2
  let captureScreen1, captureScreen2
  let isOAuth

  before(async function () { 
    console.log('Distributed Conflict Resolution before ' + other_rabbitmq_url)
    
    isOAuth = hasProfile('oauth2')

    // Initialize two separate browser instances
    // driver1 connects to the default RABBITMQ_URL
    driver1 = buildDriver()
    // driver2 connects to the OTHER_RABBITMQ_URL (node 2 in cluster)
    driver2 = buildDriver(other_rabbitmq_url)

    await goToHome(driver1)
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

    // Wait a moment for UI to settle
    await driver1.sleep(2000)

    // Check which one succeeded and which one failed
    let isLoaded1 = await overview1.isLoaded().catch(() => false)
    let isLoaded2 = await overview2.isLoaded().catch(() => false)

    let isWarningVisible1 = await login1.isWarningVisible().catch(() => false)
    let isWarningVisible2 = await login2.isWarningVisible().catch(() => false)

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
