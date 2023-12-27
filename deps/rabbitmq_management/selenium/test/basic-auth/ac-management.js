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
