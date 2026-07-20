const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const AdminTab = require('../pageobjects/AdminTab')
const FeatureFlagsAdminTab = require('../pageobjects/FeatureFlagsAdminTab')

// Both flags are stable and left disabled on boot by the suite's advanced.config.
// Enabling a feature flag cannot be undone, so each test case needs its own.
const TOGGLED_FEATURE_FLAG = 'track_qq_members_uids'
const ENABLED_VIA_BUTTON_FEATURE_FLAG = 'tie_binding_to_dest_with_keep_while_cond'

describe('Feature flags in Admin tab', function () {
  let driver
  let login
  let overview
  let ffTab
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    adminTab = new AdminTab(driver)
    ffTab = new FeatureFlagsAdminTab(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('guest', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    await overview.selectRefreshOption("Do not refresh")
    await overview.clickOnAdminTab()
    await adminTab.clickOnFeatureFlags()
  })

  it('it has at least one feature flag', async function () {
    let ffTable = await ffTab.getAll()
    assert(ffTable.length > 0)
  })

  it('it warns about disabled stable feature flags', async function () {
    assert(await ffTab.isDisabledStableWarningDisplayed())
    assert(await ffTab.isEnableAllStableFeatureFlagsEnabled())
  })

  it('it enables a stable feature flag from its toggle', async function () {
    assert(!await (await ffTab.getState(TOGGLED_FEATURE_FLAG)).isSelected())
    await ffTab.enable(TOGGLED_FEATURE_FLAG)
    await ffTab.waitUntilEnabled(TOGGLED_FEATURE_FLAG)
  })

  it('it enables the remaining stable feature flags from the warning button', async function () {
    assert(!await (await ffTab.getState(ENABLED_VIA_BUTTON_FEATURE_FLAG)).isSelected())
    await ffTab.clickOnEnableAllStableFeatureFlags()
    await ffTab.waitUntilEnabled(ENABLED_VIA_BUTTON_FEATURE_FLAG)
    assert(await ffTab.isDisabledStableWarningNotDisplayed())
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
