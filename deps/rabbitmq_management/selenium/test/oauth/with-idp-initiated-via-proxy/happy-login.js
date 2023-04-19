const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToLogin, goToHome, captureScreensFor, teardown } = require('../../utils')

const OverviewPage = require('../../pageobjects/OverviewPage')

describe('A user with a JWT token', function () {
  let overview
  let captureScreen
  let token
  let fakePortal

  before(async function () {
    driver = buildDriver()
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('can log in presenting the token on the Authorization header via fakeproxy', async function () {
    await goToHome(driver);
    await overview.isLoaded()
    assert.equal(await overview.getUser(), 'User rabbit_idp_user')
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
