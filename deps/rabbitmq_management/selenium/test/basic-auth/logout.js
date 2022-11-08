const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')

describe('When a logged in user', function () {
  let overview
  let loginPage
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    loginPage = new LoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('logs out', async function () {
    await loginPage.login('guest', 'guest')
    await overview.isLoaded()
    await overview.logout()
    await loginPage.isLoaded()
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
