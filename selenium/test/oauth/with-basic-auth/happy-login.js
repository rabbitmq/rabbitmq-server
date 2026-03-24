const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, idpLoginPage, log } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')
const OverviewPage = require('../../pageobjects/OverviewPage')

describe('An user with administrator tag', function () {
  let driver
  let homePage
  let idpLogin
  let overview
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    homePage = new SSOHomePage(driver)
    idpLogin = idpLoginPage(driver)
    overview = new OverviewPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('can log in with OAuth 2.0', async function () {
    await homePage.clickToLogin()
    await idpLogin.login('rabbit_admin', 'rabbit_admin')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    await overview.logout()
  })

  it('can log in with Basic Auth', async function () {
    await homePage.toggleBasicAuthSection()
    await homePage.basicAuthLogin('guest', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    await overview.logout()
  })

  describe("and logged in via OAuth 2.0", function() {
    before(async function() {
      await homePage.clickToLogin()
      await idpLogin.login('rabbit_admin', 'rabbit_admin')
      if (!await overview.isLoaded()) {
        throw new Error('Failed to login via OAuth 2.0')
      }
    })
    it ('can reload page without being logged out', async function() {
      log("About to refresh page")
      await overview.refresh()
      if (!await overview.isLoaded()) {
        throw new Error('Failed to keep session opened')
      }
    })
    after(async function () {
       await overview.logout()
    })
  })

  describe("and logged in via basic auth", function() {
    before(async function() {
      await homePage.toggleBasicAuthSection()
      await homePage.basicAuthLogin('guest', 'guest')
      if (!await overview.isLoaded()) {
        throw new Error('Failed to login')
      }
    })
    it ('can reload page without being logged out', async function() {
      log("About to refresh page")
      await overview.refresh()
      if (!await overview.isLoaded()) {
        throw new Error('Failed to keep session opened')
      }
    })
    after(async function () {
       await overview.logout()
    })
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
