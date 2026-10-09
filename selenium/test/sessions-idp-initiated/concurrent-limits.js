const assert = require('assert')
const { buildDriver, captureScreensFor, teardown, hasProfile, doUntil } = require('../utils')
const { getManagementUrl, basicAuthorization, deleteUserSessions, getUserSessions } = require('../mgt-api')

const OverviewPage = require('../pageobjects/OverviewPage')
const FakePortalPage = require('../pageobjects/FakePortalPage')
const SSOHomePage = require('../pageobjects/SSOHomePage')

describe('Concurrent session limit with IDP-initiated login', function () {
  let driver1, driver2
  let overview1
  let portal1, portal2
  let home2
  let captureScreen1, captureScreen2
  let username
  let password
  const adminAuth = basicAuthorization('guest', 'guest')
  this.timeout(120000)

  async function sessionCount() {
    return (await getUserSessions(getManagementUrl(), adminAuth, username)).filtered_count
  }

  async function login(portal) {
    await portal.goToHome(username, password)
    if (!await portal.isLoaded()) {
      throw new Error('Failed to load fakePortal')
    }
    await portal.login()
  }

  before(async function () {
    if (!hasProfile('sessions')) {
      return this.skip()
    }
    username = process.env.MGT_CLIENT_ID_FOR_IDP_INITIATED || 'rabbit_idp_user'
    password = process.env.MGT_CLIENT_SECRET_FOR_IDP_INITIATED || 'rabbit_idp_user'
    await deleteUserSessions(getManagementUrl(), adminAuth, username)

    driver1 = buildDriver()
    driver2 = buildDriver()
    overview1 = new OverviewPage(driver1)
    portal1 = new FakePortalPage(driver1)
    portal2 = new FakePortalPage(driver2)
    home2 = new SSOHomePage(driver2)
    captureScreen1 = captureScreensFor(driver1, __filename + '_driver1')
    captureScreen2 = captureScreensFor(driver2, __filename + '_driver2')
  })

  it('should allow the first login and reject the second when the limit is 1', async function () {
    await login(portal1)
    assert.ok(await overview1.isLoaded(20000), 'Failed to login on driver 1')

    await login(portal2)
    assert.ok(await home2.isWarningVisible(20000), 'Warning message should be visible on driver 2')
    assert.ok((await home2.getWarning()).includes('Concurrent session limit reached'))
    assert.equal(await sessionCount(), 1)
  })

  it('should free the slot when the first session logs out', async function () {
    await overview1.logout()
    await doUntil(sessionCount, (count) => count == 0, 1000, 'The session was not deleted on logout', 10)
  })

  after(async function () {
    if (driver1) await teardown(driver1, this, captureScreen1)
    if (driver2) await teardown(driver2, this, captureScreen2)
  })
})
