const assert = require('assert')
const { getOAuthBootstrap } = require('../../utils')

// Regression test for the OAuth 2 client secret disclosure (GHSA-f9f2-q3jf-wfj3).
// The unauthenticated bootstrap script must not carry management.oauth_client_secret;
// resource servers that need it are served through the server-side token endpoint proxy.
describe('The unauthenticated OAuth 2 bootstrap script', function () {
  this.timeout(20000)

  // The secret value is not checked directly because in this suite the
  // configured client id equals the client secret; the settings must not carry
  // the oauth_client_secret field at all.
  it('does not disclose the client secret', function () {
    const body = getOAuthBootstrap()
    assert.ok(!body.includes('oauth_client_secret'),
      'bootstrap.js must not contain the oauth_client_secret field')
  })

  it('advertises the token endpoint proxy instead', function () {
    const body = getOAuthBootstrap()
    assert.ok(body.includes('use_token_endpoint_proxy'),
      'bootstrap.js must mark the resource server as using the token endpoint proxy')
  })
})
