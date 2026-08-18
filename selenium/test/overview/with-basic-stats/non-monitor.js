const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../../utils')

const LoginPage = require('../../pageobjects/LoginPage')
const OverviewPage = require('../../pageobjects/OverviewPage')

describe('Overview page for a non-monitoring user with management stats disabled', function () {
  let driver, login, overview, captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('management', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
  })

  it('shows basic info instead of the stats-disabled error', async function () {
    assert.ok(await overview.isPopupWarningNotDisplayed())
    assert.ok((await overview.getMainHeadingText()).includes('Management only mode'))

    assert.ok(await overview.isSectionDisplayed('Totals'))
    assert.ok(await overview.isSubsectionDisplayed('Global counts'))
    assert.ok(await overview.isGlobalCountDisplayed('Connections'))
    assert.ok(await overview.isGlobalCountNotDisplayed('Channels'))
    assert.ok(await overview.isGlobalCountDisplayed('Exchanges'))
    assert.ok(await overview.isGlobalCountDisplayed('Queues'))
    assert.ok(await overview.isGlobalCountNotDisplayed('Consumers'))

    assert.ok(await overview.isSectionNotDisplayed('Nodes'))
    assert.ok(await overview.isSectionNotDisplayed('Churn statistics'))
    assert.ok(await overview.isSectionNotDisplayed('Ports and contexts'))
    assert.ok(await overview.isSectionNotDisplayed('Export definitions'))
    assert.ok(await overview.isSectionNotDisplayed('Import definitions'))
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
