const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const AdminTab = require('../pageobjects/AdminTab')

describe('management user without any vhosts permissions', function () {
  let homePage
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
    assert.rejects(overview.waitForConnectionsTab())
    assert.rejects(overview.waitForChannelsTab())
    assert.rejects(overview.waitForQueuesTab())
    assert.rejects(overview.waitForExchangesTab())
    assert.rejects(overview.waitForAdminTab())
    assert.rejects(overview.waitForStreamConnectionsTab())
  })

  it('cannot see nor choose any available vhost', async function () {
    assert.rejects(overview.getSelectableVhosts())
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
