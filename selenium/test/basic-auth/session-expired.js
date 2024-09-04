const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, delay } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')

describe('Once user is logged in', function () {
  let homePage
  let idpLogin
  let overview
  let captureScreen
  this.timeout(65000) // hard-coded to 25secs because this test requires 35sec to run

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
    await login.login('guest', 'guest')
    await overview.isLoaded()

  })

  it('it has to login after the session expires', async function () {
    
    await delay(60000)
    await login.isLoaded() 
    await login.login('guest', 'guest')
    await overview.isLoaded() 
    await overview.clickOnConnectionsTab() // and we can still interact with the ui
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
