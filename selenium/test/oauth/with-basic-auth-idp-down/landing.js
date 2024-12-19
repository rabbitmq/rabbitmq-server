const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, teardown, captureScreensFor } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')

describe('When basic authentication is enabled but UAA is down', function () {
  let driver
  let homePage
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    homePage = new SSOHomePage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('should display warning message that UAA is down', async function () {
    await homePage.isLoaded()
    const message = await homePage.getWarning()
    assert.ok(message.startsWith('OAuth resource [rabbitmq] not available'))
    assert.ok(message.endsWith(' not reachable'))
  })

  it('should not be presented oauth2 section', async function () {
    await homePage.isLoaded()
    assert.ok(await homePage.isOAuth2SectionNotVisible())
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
