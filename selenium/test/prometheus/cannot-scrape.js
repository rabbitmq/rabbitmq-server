const assert = require('assert')
const XMLHttpRequest = require('xmlhttprequest').XMLHttpRequest
const { info, mask, tokenFor, openIdConfiguration } = require('../utils')

const managementUrl = process.env.RABBITMQ_URL || 'http://rabbitmq:15672/'
const prometheusUrl = process.env.PROMETHEUS_URL ||
  managementUrl.replace('15672', '15692').replace(/\/$/, '')

const oauthProviderUrl = process.env.OAUTH_PROVIDER_URL
const clientId = process.env.PROMETHEUS_MANAGEMENT_CLIENT_ID
const clientSecret = process.env.PROMETHEUS_MANAGEMENT_CLIENT_SECRET

describe('Prometheus endpoint with a management-only OAuth2 client (no monitoring tag)', function () {
  let token

  before(function () {
    info('prometheusUrl: ' + prometheusUrl)
    info('oauthProviderUrl: ' + oauthProviderUrl)
    info('clientId: ' + clientId + ', clientSecret: ' + mask(clientSecret))
    assert.ok(oauthProviderUrl, 'OAUTH_PROVIDER_URL env var must be set')
    assert.ok(clientId, 'PROMETHEUS_MANAGEMENT_CLIENT_ID env var must be set')
    assert.ok(clientSecret, 'PROMETHEUS_MANAGEMENT_CLIENT_SECRET env var must be set')
    const openIdConfig = openIdConfiguration(oauthProviderUrl)
    info('token_endpoint: ' + openIdConfig.token_endpoint)
    token = tokenFor(clientId, clientSecret, openIdConfig.token_endpoint)
    info('Obtained access token: ' + mask(token))
  })

  it('cannot scrape /metrics', function () {
    const req = new XMLHttpRequest()
    req.open('GET', prometheusUrl + '/metrics', false)
    req.setRequestHeader('Authorization', 'Bearer ' + token)
    req.send()
    assert.equal(401, req.status)
  })
})
