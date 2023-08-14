const assert = require('assert')
const { getURLForProtocol } = require('../utils')
const {execSync} = require('child_process')

const profiles = process.env.PROFILES || ""
var backends = ""
for (const element of profiles.split(" ")) {
  if ( element.startsWith("auth_backends-") ) {
    backends = element.substring(element.indexOf("-")+1)
  }
}

describe('Having the following auth_backends enabled: ' + backends, function () {

  before(function () {

  })

  it('can open an AMQP 1.0 connection', function () {
    execSync("npm run amqp10_roundtriptest")
  })

  after(function () {

  })
})
