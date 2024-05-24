const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const AdminTab = require('../pageobjects/AdminTab')
const LimitsAdminTab = require('../pageobjects/LimitsAdminTab')

describe('management user with vhosts permissions', function () {
  let homePage
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    admin = new AdminTab(driver)
    limits = new LimitsAdminTab(driver)
    captureScreen = captureScreensFor(driver, __filename)
    await login.login('management', 'guest')
    await overview.isLoaded()
  })

  it('can access overview tab', async function () {
    await overview.clickOnOverviewTab()
    await overview.waitForOverviewTab()
    assert.ok(!await overview.isPopupWarningDisplayed())
  })
  it('can access connections tab', async function () {
    await overview.clickOnConnectionsTab()
    await overview.waitForConnectionsTab()
    assert.ok(!await overview.isPopupWarningDisplayed())
  })
  it('can access channels tab', async function () {
    await overview.clickOnChannelsTab()
    await overview.waitForChannelsTab()
    assert.ok(!await overview.isPopupWarningDisplayed())
  })
  it('can access exchanges tab', async function () {
    await overview.clickOnExchangesTab()
    await overview.waitForExchangesTab()
    assert.ok(!await overview.isPopupWarningDisplayed())
  })
  it('can access queues and streams tab', async function () {
    await overview.clickOnQueuesTab()
    await overview.waitForQueuesTab()
    assert.ok(!await overview.isPopupWarningDisplayed())
  })
  it('can access limited options in admin tab', async function () {    
    await overview.clickOnAdminTab()
    await overview.waitForAdminTab()
    assert.ok(!await overview.isPopupWarningDisplayed())
  })

  it('cannot add/update user limits', async function () {
    await overview.clickOnAdminTab()
    await admin.clickOnLimits()
    await limits.list_virtual_host_limits()
    assert.rejects(limits.list_user_limits())
  })


  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
