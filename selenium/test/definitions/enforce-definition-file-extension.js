const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, hasProfile } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')

describe('Enforce definition file extension', function () {
  let driver
  let login
  let overview
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('guest', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
  })

  it('file input accepts only .json files', async function () {
    if (!hasProfile('enforce-definition-file-extension')) {
      return this.skip()
    }
    let accept = await overview.getFileInputAcceptAttribute()
    assert.equal('.json', accept)
  })

  it('rejects upload of a file with a non-.json extension', async function () {
    if (!hasProfile('enforce-definition-file-extension')) {
      return this.skip()
    }
    let message = await overview.uploadBrokerDefinitionsExpectingError(
      process.cwd() + '/test/definitions/sample-definitions.txt'
    )
    assert.ok(
      message.indexOf('Only .json files are accepted for definitions import.') !== -1,
      'Expected error message about .json extension, got: ' + message
    )
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
