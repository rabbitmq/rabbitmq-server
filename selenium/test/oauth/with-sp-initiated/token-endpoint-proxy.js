const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../../utils')

// Regression test for the OAuth 2 client secret disclosure (GHSA-f9f2-q3jf-wfj3).
// The unauthenticated bootstrap script must not carry management.oauth_client_secret;
// resource servers that need it are served through the server-side token endpoint proxy.
// The script is fetched through the browser so it uses the browser's trust of the
// management UI's TLS certificate.
describe('The unauthenticated OAuth 2 bootstrap script', function () {
  let driver
  let captureScreen
  this.timeout(30000)

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  async function fetchBootstrap () {
    return driver.driver.executeAsyncScript(function () {
      const callback = arguments[arguments.length - 1]
      fetch('js/oidc-oauth/bootstrap.js')
        .then(function (response) { return response.text() })
        .then(callback)
        .catch(function (err) { callback('FETCH_ERROR: ' + err) })
    })
  }

  it('does not disclose the client secret', async function () {
    const body = await fetchBootstrap()
    assert.ok(!body.includes('oauth_client_secret'),
      'bootstrap.js must not contain the oauth_client_secret field')
  })

  it('advertises the token endpoint proxy instead', async function () {
    const body = await fetchBootstrap()
    assert.ok(body.includes('use_token_endpoint_proxy'),
      'bootstrap.js must mark the resource server as using the token endpoint proxy')
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
