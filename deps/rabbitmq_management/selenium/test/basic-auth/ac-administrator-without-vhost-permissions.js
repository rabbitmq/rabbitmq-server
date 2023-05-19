const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const AdminTab = require('../pageobjects/AdminTab')

describe('administrator user without any vhosts permissions', function () {
  let homePage
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    admin = new AdminTab(driver)
    captureScreen = captureScreensFor(driver, __filename)
    await login.login('administrator-only', 'guest')
    await overview.isLoaded()
  })

  it('can access all menu options', async function () {
    await overview.clickOnConnectionsTab()
    await overview.clickOnChannelsTab()
    await overview.clickOnQueuesTab()
    await overview.clickOnExchangesTab()
    await overview.clickOnAdminTab()
    await overview.clickOnStreamTab()
  })
  it('can access all Admin menu options', async function () {
    await overview.clickOnAdminTab()
    await admin.clickOnUsers()
    await admin.clickOnVhosts()
    await admin.clickOnPolicies()
    await admin.clickOnLimits()
    await admin.clickOnFeatureFlags()
    await admin.clickOnCluster()
    await admin.clickOnFederationStatus()
    await admin.clickOnFederationUpstreams()
    await admin.clickOnShovelStatus()
    await admin.clickOnShovelManagement()
    await admin.clickOnTopProcesses()
    await admin.clickOnTopEtsTable()
    await admin.clickOnTracing()
  })

  it('can choose from any available vhost', async function () {
    vhosts = await overview.getSelectableVhosts()
    assert.ok(vhosts.includes("/"))
    assert.ok(vhosts.includes("another"))
    assert.ok(vhosts.includes("All"))
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
