const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../../utils')

const LoginPage = require('../../pageobjects/LoginPage')
const OverviewPage = require('../../pageobjects/OverviewPage')
const ConnectionsTab = require('../../pageobjects/ConnectionsTab')

describe('Given an amqp10 connection is selected', function () {
  let homePage
  let captureScreen
  let connections

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    connections = new ConnectionsTab(driver)
    captureScreen = captureScreensFor(driver, __filename)
    await login.login('management', 'guest')
    await overview.isLoaded()

    await overview.clickOnConnectionsTab()
    await connections.clickOnConnection()
  })

  it('can list session information', async function () {
    // flow control state
  })
  
  it('can list link information', async function () {
    // names 
    // target and source information
    // unconfirmed messages 
    // flow control
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
