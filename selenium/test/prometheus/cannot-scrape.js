const assert = require('assert')
const XMLHttpRequest = require('xmlhttprequest').XMLHttpRequest
const { log, error, tokenFor, openIdConfiguration } = require('../utils')

const managementUrl = process.env.RABBITMQ_URL || 'http://rabbitmq:15672/'
const prometheusUrl = process.env.PROMETHEUS_URL ||
  managementUrl.replace('15672', '15692').replace(/\/$/, '')

const oauthProviderUrl = process.env.OAUTH_PROVIDER_URL
const clientId = process.env.PROMETHEUS_MANAGEMENT_CLIENT_ID
const clientSecret = process.env.PROMETHEUS_MANAGEMENT_CLIENT_SECRET

describe('Prometheus endpoint with a management-only OAuth2 client (no monitoring tag)', function () {
  let token

  before(function () {
    log('oauthProviderUrl: ' + oauthProviderUrl)
    log('clientId: ' + clientId)
    const openIdConfig = openIdConfiguration(oauthProviderUrl)
    log('token_endpoint: ' + openIdConfig.token_endpoint)
    token = tokenFor(clientId, clientSecret, openIdConfig.token_endpoint)
    log('Obtained access token')
  })

  it('cannot scrape /metrics', function () {
    const req = new XMLHttpRequest()
    req.open('GET', prometheusUrl + '/metrics', false)
    req.setRequestHeader('Authorization', 'Bearer ' + token)
    req.send()
    assert.equal(401, req.status)
  })
})
