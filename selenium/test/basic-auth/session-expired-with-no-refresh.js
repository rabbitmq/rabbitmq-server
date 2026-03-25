const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, delay } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')

describe('Once user is logged in and no refresh is configured', function () {
  let driver
  let login
  let overview
  let captureScreen
  this.timeout(65000)

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
    await login.login('guest', 'guest')
    await overview.isLoaded()
    await overview.selectRefreshOption("Do not refresh")
    // Trigger a UI preference change to verify it does not extend the session.
    await overview.ensureTotalsSectionIsInvisible()
  })

  it('any authorized request after the session has expired should log the user out', async function () {
    await delay(60000)
    await overview.clickOnConnectionsTab()
    await login.isLoaded()
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
