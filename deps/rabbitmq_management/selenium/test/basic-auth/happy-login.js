const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')

describe('An internal user with administrator tag', function () {
  let login
  let overview
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('can log in into the management ui', async function () {
    await login.login('guest', 'guest')
    await overview.isLoaded()
    assert.equal(await overview.getUser(), 'User guest')
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
