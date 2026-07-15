const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, goToExchanges, goToQueues, captureScreensFor, teardown } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')
const FakePortalPage = require('../../pageobjects/FakePortalPage')
const ExchangesPage = require('../../pageobjects/ExchangesPage')
const QueuesAndStreamsPage = require('../../pageobjects/QueuesAndStreamsPage')
const OverviewPage = require('../../pageobjects/OverviewPage')

// Exercises the oauth-return-to behaviour for the idp_initiated flow: after
// logging in, the user should land back on the page they were on when they
// clicked "log in", and a plain visit to "/" must never be hijacked to a saved
// page. Each describe uses a fresh browser so it starts logged out.

describe('A user who logs in from the home page', function () {
  let driver
  let homePage
  let fakePortal
  let overview
  let captureScreen

  before(async function () {
    driver = buildDriver()
    homePage = new SSOHomePage(driver)
    fakePortal = new FakePortalPage(driver)
    overview = new OverviewPage(driver)
    await goToHome(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('should land on the home page', async function () {
    await homePage.clickToLogin()
    if (!await fakePortal.isLoaded()) {
      throw new Error('Failed to load fakePortal')
    }
    await fakePortal.login()
    if (!await overview.isLoaded()) {
      throw new Error('Home page did not load after login')
    }
    const url = await driver.driver.getCurrentUrl()
    assert.ok(url.endsWith('#/'),
      'Expected to land on the home page (#/) but got ' + url)
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})

describe('A user who accesses a protected page before logging in', function () {
  let driver
  let homePage
  let fakePortal
  let exchanges
  let overview
  let captureScreen

  before(async function () {
    driver = buildDriver()
    homePage = new SSOHomePage(driver)
    fakePortal = new FakePortalPage(driver)
    exchanges = new ExchangesPage(driver)
    overview = new OverviewPage(driver)
    await goToExchanges(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('should be redirected back to that page after logging in', async function () {
    await homePage.clickToLogin()
    if (!await fakePortal.isLoaded()) {
      throw new Error('Failed to load fakePortal')
    }
    await fakePortal.login()
    if (!await exchanges.isLoaded()) {
      throw new Error('Was not redirected back to the exchanges page after login')
    }
    await exchanges.getPagingSectionHeaderText()
  })

  it('should go to the home page when later visiting "/" directly', async function () {
    await goToHome(driver)
    if (!await overview.isLoaded()) {
      throw new Error('Home page did not load')
    }
    const url = await driver.driver.getCurrentUrl()
    assert.ok(url.endsWith('#/'),
      'Expected to land on the home page (#/) but got ' + url)
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})

describe('A user who visits several pages before logging in', function () {
  let driver
  let homePage
  let fakePortal
  let queuesAndStreams
  let captureScreen

  before(async function () {
    driver = buildDriver()
    homePage = new SSOHomePage(driver)
    fakePortal = new FakePortalPage(driver)
    queuesAndStreams = new QueuesAndStreamsPage(driver)
    // Access exchanges first, then move to queues while still logged out. Queues
    // is the page the user ends up on, so that is the page that must be restored.
    await goToExchanges(driver)
    await goToQueues(driver)
    captureScreen = captureScreensFor(driver, __filename)
  })

  it('should be redirected to the last page it visited', async function () {
    await homePage.clickToLogin()
    if (!await fakePortal.isLoaded()) {
      throw new Error('Failed to load fakePortal')
    }
    await fakePortal.login()
    if (!await queuesAndStreams.isLoaded()) {
      throw new Error('Was not redirected back to the queues page after login')
    }
    await queuesAndStreams.getPagingSectionHeaderText()
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
