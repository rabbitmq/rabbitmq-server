const assert = require('assert')
const { buildDriver, captureScreensFor, teardown, hasProfile } = require('../utils')
const { getManagementUrl, basicAuthorization, deleteUserSessions, getUserSessions } = require('../mgt-api')

const OverviewPage = require('../pageobjects/OverviewPage')
const FakePortalPage = require('../pageobjects/FakePortalPage')
const SSOHomePage = require('../pageobjects/SSOHomePage')

describe('When an administrator deletes the session of a user logged in via IDP-initiated login', function () {
  let driver
  let overview
  let portal
  let homePage
  let captureScreen
  let username
  let password
  const adminAuth = basicAuthorization('guest', 'guest')
  this.timeout(180000)

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

  it('it is sent back to the login page with a warning', async function () {
    await portal.goToHome(username, password)
    await portal.login()
    assert.ok(await overview.isLoaded(20000), 'Failed to login')
    assert.equal(await sessionCount(), 1)

    await deleteUserSessions(getManagementUrl(), adminAuth, username)
    assert.equal(await sessionCount(), 0)

    assert.ok(await homePage.isWarningVisible(60000), 'A warning should be visible on the login page')
    assert.ok((await homePage.getWarning()).includes('Session terminated or expired'))
  })

  after(async function () {
    if (driver) await teardown(driver, this, captureScreen)
  })
})
