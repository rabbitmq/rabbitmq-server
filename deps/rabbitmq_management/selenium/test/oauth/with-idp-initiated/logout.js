const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToLogin, tokenFor, captureScreensFor, teardown } = require('../../utils')

const OverviewPage = require('../../pageobjects/OverviewPage')
const FakePortalPage = require('../../pageobjects/FakePortalPage')

describe('When a logged in user', function () {
  let overview
  let fakePortal
  let captureScreen

  before(async function () {
    driver = buildDriver()
    fakePortal = new FakePortalPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await fakePortal.goToHome()
    if (!await fakePortal.isLoaded()) {
      throw new Error('Failed to load fakePortal')
    }
    await fakePortal.login()
    await overview.isLoaded()
  })

  it('logs out', async function () {
    await overview.logout()
    await fakePortal.isLoaded()
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
