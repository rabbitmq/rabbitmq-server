const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToLogin, goTo, tokenFor, captureScreensFor, teardown } = require('../../utils')

const OverviewPage = require('../../pageobjects/OverviewPage')
const FakePortalPage = require('../../pageobjects/FakePortalPage')

describe('A user with a JWT token', function () {
  let overview
  let captureScreen
  let token
  let fakePortal

  before(async function () {
    driver = buildDriver()
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
    fakePortal = new FakePortalPage(driver)
  })

  it('can log in presenting the token to the /login URL via fakeportal', async function () {
    await fakePortal.goToHome("rabbit_idp_user", "rabbit_idp_user")
    if (!await fakePortal.isLoaded()) {
      throw new Error('Failed to load fakePortal')
    }
    await fakePortal.login()
    await overview.isLoaded()
    assert.equal(await overview.getUser(), 'User rabbit_idp_user')
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
