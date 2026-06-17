const assert = require('assert')
const { buildDriver, goToHome, goToQueue, captureScreensFor, teardown } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')

describe('management user accessing an existing but restricted vhost', function () {
  let driver
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
    await login.login('management', 'guest')
    await overview.isLoaded()
  })

  it('sees a 404 page instead of an authorization error when accessing a restricted vhost', async function () {
    // The 'other' vhost exists but the 'management' user has no permissions on it.
    // Navigating to any resource within it must render a 404 page, not a popup
    // warning, so that the existence of the vhost is not disclosed to the user.
    await goToQueue(driver, 'other', 'probe')
    assert.equal(await overview.getMainHeadingText(), 'Not found')
    assert.ok(await overview.isPopupWarningNotDisplayed())
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
