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
    await overview.waitForOverviewTab()
    await overview.waitForConnectionsTab()
    await overview.waitForChannelsTab()
    await overview.waitForQueuesTab()
    await overview.waitForExchangesTab()
    await overview.waitForAdminTab()
    await overview.waitForStreamConnectionsTab()
  })
  it('can access all Admin menu options', async function () {
    await overview.clickOnAdminTab()
    await admin.waitForUsersMenuOption()
    await admin.waitForLimitsMenuOption()
    await admin.waitForVhostsMenuOption()
    await admin.waitForPoliciesMenuOption()
    await admin.waitForFeatureFlagsMenuOption()
    await admin.waitForClusterMenuOption()
    await admin.waitForFederationStatusMenuOption()
    await admin.waitForFederationUpstreamsMenuOption()
    await admin.waitForShovelStatusMenuOption()
    await admin.waitForShovelManagementMenuOption()
    await admin.waitForTopProcessesMenuOption()
    await admin.waitForTopEtsTableMenuOption()
    await admin.waitForTracingMenuOption()

  })

  it('can choose from any available vhost', async function () {
    vhosts = await overview.getSelectableVhosts()
    assert.ok(vhosts.includes("/"))
    assert.ok(vhosts.includes("All"))
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
