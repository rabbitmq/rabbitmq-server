const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, captureScreensFor, teardown } = require('../../utils')

const OverviewPage = require('../../pageobjects/OverviewPage')
const FakePortalPage = require('../../pageobjects/FakePortalPage')

describe('A user with a JWT token', function () {
  let driver
  let overview
  let captureScreen
  let fakePortal
  let username
  let password

  before(async function () {
    username = process.env.MGT_CLIENT_ID_FOR_IDP_INITIATED || 'rabbit_idp_user'
    password = process.env.MGT_CLIENT_SECRET_FOR_IDP_INITIATED || 'rabbit_idp_user'
    driver = buildDriver()
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
    fakePortal = new FakePortalPage(driver)
  })

  it('can log in presenting the token to the /login URL via fakeportal', async function () {
    await fakePortal.goToHome(username, password)
    if (!await fakePortal.isLoaded()) {
      throw new Error('Failed to load fakePortal')
    }
    await fakePortal.login()
    await overview.isLoaded()
    assert.equal(await overview.getUser(), 'User ' + username)
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
