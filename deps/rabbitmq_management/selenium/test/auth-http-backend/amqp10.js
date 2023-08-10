const assert = require('assert')
const { getURLForProtocol } = require('../utils')
const {execSync} = require('child_process')


describe('AMQP1.0 client authenticate with basic auth against http backend', function () {

  before(function () {

  })

  it('can open a connection', function () {
    execSync("npm run amqp10_roundtriptest")  
  })

  after(function () {

  })
})
