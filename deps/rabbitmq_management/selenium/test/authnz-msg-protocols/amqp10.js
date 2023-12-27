const assert = require('assert')
const { getURLForProtocol } = require('../utils')
const { reset, expectUser, expectVhost, expectResource, allow, verifyAll } = require('../mock_http_backend')
const {execSync} = require('child_process')

const profiles = process.env.PROFILES || ""
var backends = ""
for (const element of profiles.split(" ")) {
  if ( element.startsWith("auth_backends-") ) {
    backends = element.substring(element.indexOf("-")+1)
  }
}

describe('Having AMQP 1.0 protocol enabled and the following auth_backends: ' + backends, function () {
  let expectations = []
  let username = process.env.RABBITMQ_AMQP_USERNAME
  let password = process.env.RABBITMQ_AMQP_PASSWORD

  before(function () {
    if (backends.includes("http") && username.includes("http")) {
      reset()
      expectations.push(expectUser({ "username": username, "password": password}, "allow"))
      expectations.push(expectVhost({ "username": username, "vhost": "/"}, "allow"))
      expectations.push(expectResource({ "username": username, "vhost": "/", "resource": "queue", "name": "my-queue", "permission":"configure", "tags":""}, "allow"))
      expectations.push(expectResource({ "username": username, "vhost": "/", "resource": "queue", "name": "my-queue", "permission":"read", "tags":""}, "allow"))
      expectations.push(expectResource({ "username": username, "vhost": "/", "resource": "exchange", "name": "amq.default", "permission":"write", "tags":""}, "allow"))
    }
  })

  it('can open an AMQP 1.0 connection', function () {
    execSync("npm run amqp10_roundtriptest")

  })

  after(function () {
      if ( backends.includes("http") ) {
        verifyAll(expectations)
      }
  })
})
