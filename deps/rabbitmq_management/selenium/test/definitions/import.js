const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, delay } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const AdminTab = require('../pageobjects/AdminTab')

describe('Import definititions', function () {
  let login
  let overview
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    adminTab = new AdminTab(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('guest', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
  })

  it('is allowed to administrator users', async function () {
    let message = await overview.uploadBrokerDefinitions(process.cwd() + "/test/definitions/import-newguest-user.json")
    assert.equal(true, message.indexOf('Your definitions were imported successfully.') !== -1)
    await overview.clickOnAdminTab()
    assert.equal(true, await adminTab.searchForUser("newguest"))
  })


  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
