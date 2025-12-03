const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, assertAllOptions, hasProfile } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')


describe('Given three oauth resources but only two enabled, an unauthenticated user', function () {
  let homePage
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    homePage = new SSOHomePage(driver)
    captureScreen = captureScreensFor(driver, __filename)
    await homePage.isLoaded()
  })

  it('should be presented with a login button to log in using OAuth 2.0', async function () {
    await homePage.getOAuth2Section()
    assert.equal(await homePage.getLoginButton(), 'Click here to log in')
  })

  it('there should be two OAuth resources to choose from', async function () {
    resources = await homePage.getOAuthResourceOptions()
    if (hasProfile("with-resource-label")) {
      assertAllOptions([
        { value : "rabbit_prod", text : "RabbitMQ Production" },
        { value : "rabbit_dev", text : "RabbitMQ Development" }
        ], resources);       
    }else {
      assertAllOptions([
        { value : "rabbit_prod", text : "rabbit_prod" },
        { value : "rabbit_dev", text : "rabbit_dev" }
        ], resources);       
    }    
  })

  it('there should preserve the same order as they were configured (prod then dev)', async function () {
    resources = await homePage.getOAuthResourceOptions()
    if (hasProfile("with-resource-label")) {
        // assert resources are rendered in the same order they are configured : prod and then dev
        assert.equal("RabbitMQ Production", resources[0].text);
        assert.equal("RabbitMQ Development", resources[1].text);
    }else {
        assert.equal("rabbit_prod", resources[0].text);
        assert.equal("rabbit_dev", resources[1].text);
    }    
  })

  it('should not be presented with a login button to log in using Basic Auth', async function () {
    assert.ok(!await homePage.isBasicAuthSectionVisible())
  })

  it('should not have a warning message', async function () {
    assert.ok(!await homePage.isWarningVisible())
  })


  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
