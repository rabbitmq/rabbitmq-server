const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, delay } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')
const OverviewPage = require('../../pageobjects/OverviewPage')

describe('When basic authentication is enabled but UAA is down', function () {
  let driver
  let homePage
  let overview
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    homePage = new SSOHomePage(driver)    
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('can log in with Basic Auth', async function () {
    await homePage.toggleBasicAuthSection()
    assert.ok(await homePage.isLoginButtonVisible)
    await homePage.basicAuthLogin('guest', 'guest')
    await overview.isLoaded()
    assert.equal(await overview.getUser(), 'User guest')
    await overview.logout()
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
