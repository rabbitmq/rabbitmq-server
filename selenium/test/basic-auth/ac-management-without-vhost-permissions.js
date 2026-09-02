const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const AdminTab = require('../pageobjects/AdminTab')

describe('management user without any vhosts permissions', function () {
  let driver
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    admin = new AdminTab(driver)
    captureScreen = captureScreensFor(driver, __filename)
    await login.login('management-only', 'guest')
    await overview.isLoaded()
  })

  it('can only access overview', async function () {
    assert.ok(await overview.isConnectionsTabNotDisplayed())
    assert.ok(await overview.isChannelsTabNotDisplayed())
    assert.ok(await overview.isQueuesTabNotDisplayed())
    assert.ok(await overview.isExchangesTabNotDisplayed())
    assert.ok(await overview.isAdminTabNotDisplayed())
    assert.ok(await overview.isStreamConnectionsTabNotDisplayed())
  })

  it('cannot see nor choose any available vhost', async function () {
    assert.ok(await overview.isSelectableVhostsNotDisplayed())
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
