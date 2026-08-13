const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, idpLoginPage, hasProfile } = require('../utils')

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

  before(async function () {
    this.timeout(120000)
    // Check if we are running an OAuth profile
    isOAuth = hasProfile('oauth2')

    // Initialize two separate browser instances sequentially to avoid overwhelming Selenium Server
    driver1 = buildDriver(process.env.RABBITMQ_URL || 'http://localhost:15672/')
    await driver1.driver.sleep(2000)
    console.log('Driver 1 connecting to:', driver1.baseUrl)
    await goToHome(driver1)
    console.log('Driver 1 connected')
    
    driver2 = buildDriver(process.env.RABBITMQ_URL || 'http://localhost:15672/')
    await driver2.driver.sleep(2000)
    console.log('Driver 2 connecting to:', driver2.baseUrl)
    await goToHome(driver2)
    console.log('Driver 2 connected')

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
      console.log('Cleaning up sessions for ' + management_username)
      await deleteUserSessions(getManagementUrl(), adminAuth, management_username)
      await new Promise(r => setTimeout(r, 1000)) // Give cluster gossip time to propagate
      console.log('Sessions cleaned up for ' + management_username)
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

  it('should allow first login and block second login when limit is 1', async function () {
    // 1. Login with Browser 1
    console.log('Logging in with driver 1')
    await performLogin(driver1, login1, management_username, management_password)
    console.log('Checking if overview is loaded for driver 1')
    if (!await overview1.isLoaded(20000)) {
      throw new Error('Failed to login on driver 1')
    }
    console.log('Logging in with driver 2')
    // 2. Attempt login with Browser 2
    await performLogin(driver2, login2, management_username, management_password)
    
    // Verify Browser 2 is blocked and shows the warning message
    let isWarningVisible = await login2.isWarningVisible(20000)
    assert.ok(isWarningVisible, 'Warning message should be visible on driver 2')
    
    let warningText = await login2.getWarning()
    assert.ok(warningText.includes('Concurrent session limit reached'), 'Should show limit reached message')
  })

  it('should allow login after the first session logs out', async function () {
    // 1. Logout from Browser 1
    await overview1.logout()
    
    // Wait for logout to complete
    await login1.isLoaded(20000)

    // 2. Attempt login with Browser 2 again
    // For OAuth, the previous failed attempt might have left us on the login page or an error page.
    // Let's ensure we are on the home page.
    await goToHome(driver2)
    await performLogin(driver2, login2, management_username, management_password)
    
    // Verify successful login on Browser 2
    if (!await overview2.isLoaded(20000)) {
      throw new Error('Failed to login on driver 2 after driver 1 logged out')
    }
  })

  after(async function () {
    await teardown(driver1, this, captureScreen1)
    await teardown(driver2, this, captureScreen2)
  })
})
