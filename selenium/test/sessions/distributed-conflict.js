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
    if (!other_rabbitmq_url) {
      this.skip() // Skip if not running in a cluster environment
    }

    isOAuth = hasProfile('oauth2')

    // Initialize two separate browser instances
    // driver1 connects to the default RABBITMQ_URL
    driver1 = buildDriver()
    // driver2 connects to the OTHER_RABBITMQ_URL (node 2 in cluster)
    driver2 = buildDriver(other_rabbitmq_url)

    await goToHome(driver1)
    
    // For driver2, we need a custom goToHome equivalent since it targets a different URL
    await driver2.get(other_rabbitmq_url)

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

  it('should enforce limits across the cluster', async function () {
    // 1. Login to Node 1 with Browser 1
    await performLogin(driver1, login1, management_username, management_password)
    
    if (!await overview1.isLoaded()) {
      throw new Error('Failed to login on Node 1')
    }

    // Wait a moment for gossip to propagate across the cluster
    await driver1.sleep(2000)

    // 2. Attempt login to Node 2 with Browser 2
    await performLogin(driver2, login2, management_username, management_password)
    
    // Verify Browser 2 is blocked on Node 2
    let isWarningVisible = await login2.isWarningVisible()
    assert.ok(isWarningVisible, 'Warning message should be visible on Node 2')
    
    let warningText = await login2.getWarning()
    assert.ok(warningText.includes('Concurrent session limit reached'), 'Should show limit reached message on Node 2')
  })

  after(async function () {
    if (driver1) await teardown(driver1, this, captureScreen1)
    if (driver2) await teardown(driver2, this, captureScreen2)
  })
})
