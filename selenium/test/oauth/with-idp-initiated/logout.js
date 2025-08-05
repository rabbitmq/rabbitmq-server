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
  let username
  let password

  before(async function () {
    driver = buildDriver()
    username = process.env.MGT_CLIENT_ID_FOR_IDP_INITIATED || 'rabbit_idp_user'
    password = process.env.MGT_CLIENT_SECRET_FOR_IDP_INITIATED || 'rabbit_idp_user'
    
    fakePortal = new FakePortalPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await fakePortal.goToHome(username, password)
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
