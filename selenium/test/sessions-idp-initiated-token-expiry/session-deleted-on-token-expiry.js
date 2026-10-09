const assert = require('assert')
const { buildDriver, captureScreensFor, teardown, hasProfile } = require('../utils')
const { getManagementUrl, basicAuthorization, deleteUserSessions, getUserSessions } = require('../mgt-api')

const OverviewPage = require('../pageobjects/OverviewPage')
const FakePortalPage = require('../pageobjects/FakePortalPage')
const SSOHomePage = require('../pageobjects/SSOHomePage')

describe('When the token of a user logged in via IDP-initiated login expires', function () {
  let driver
  let overview
  let portal
  let homePage
  let captureScreen
  let username
  let password
  const adminAuth = basicAuthorization('guest', 'guest')
  this.timeout(120000)

  async function sessionCount() {
    return (await getUserSessions(getManagementUrl(), adminAuth, username)).filtered_count
  }

  before(async function () {
    if (!hasProfile('sessions')) {
      return this.skip()
    }
    username = process.env.MGT_CLIENT_ID_FOR_IDP_INITIATED || 'rabbit_idp_user'
    password = process.env.MGT_CLIENT_SECRET_FOR_IDP_INITIATED || 'rabbit_idp_user'
    await deleteUserSessions(getManagementUrl(), adminAuth, username)

    driver = buildDriver()
    overview = new OverviewPage(driver)
    portal = new FakePortalPage(driver)
    homePage = new SSOHomePage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('the session no longer exists in the server', async function () {
    await portal.goToHome(username, password)
    await portal.login()
    assert.ok(await overview.isLoaded(20000), 'Failed to login')
    assert.equal(await sessionCount(), 1)

    assert.ok(await homePage.isLoaded(60000), 'The user should be sent back to the login page')
    assert.equal(await sessionCount(), 0)
  })

  it('the user is shown that the token has expired', async function () {
    assert.ok(await homePage.isWarningVisible(20000), 'A warning should be visible on the login page')
    assert.match(await homePage.getWarning(), /token.*expired/i)
  })

  after(async function () {
    if (driver) await teardown(driver, this, captureScreen)
  })
})
