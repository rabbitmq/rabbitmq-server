const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../../utils')

// The unauthenticated bootstrap script must not expose
// management.oauth_client_secret. Fetched through the browser so it uses the
// browser's trust of the management UI's TLS certificate.
describe('The unauthenticated OAuth 2 bootstrap script', function () {
  let driver
  let captureScreen
  this.timeout(30000)

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('does not disclose the client secret', async function () {
    const body = await driver.driver.executeAsyncScript(function () {
      const callback = arguments[arguments.length - 1]
      fetch('js/oidc-oauth/bootstrap.js')
        .then(function (response) { return response.text() })
        .then(callback)
        .catch(function (err) { callback('FETCH_ERROR: ' + err) })
    })
    assert.ok(!body.includes('oauth_client_secret'),
      'bootstrap.js must not contain the oauth_client_secret field')
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
