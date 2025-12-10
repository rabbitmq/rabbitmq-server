const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, goToLogin, captureScreensFor, teardown, findOption } = require('../../utils')

const SSOHomePage = require('../../pageobjects/SSOHomePage')

describe('Given two oauth resources and basic auth enabled, an unauthenticated user', function () {
  let driver;
  let captureScreen;
  let login;

  before(async function () {
    this.driver = buildDriver();
    this.captureScreen = captureScreensFor(this.driver, __filename);

    login = async (key, value) => {
        await goToLogin(this.driver, key, value);
        const homePage = new SSOHomePage(this.driver);
        await homePage.isLoaded();
        return homePage;
    }
  })

  it('can preselect rabbit_dev oauth2 resource', async function () {    
    const homePage = await login("preferred_auth_mechanism", "oauth2:rabbit_dev");
    
    const oauth2Section = await homePage.isOAuth2SectionVisible();
    assert.ok((await oauth2Section.getAttribute("class")).includes("section-visible"))
    const basicSection = await homePage.isBasicAuthSectionVisible();
    assert.ok((await basicSection.getAttribute("class")).includes("section-invisible"))
        
    resources = await homePage.getOAuthResourceOptions();
    const option = findOption("rabbit_dev", resources);
    assert.ok(option);
    assert.ok(option.selected);
    
  })
  it('can preselect rabbit_prod oauth2 resource', async function () {    
    const homePage = await login("preferred_auth_mechanism", "oauth2:rabbit_prod");
    
    const oauth2Section = await homePage.isOAuth2SectionVisible();
    assert.ok((await oauth2Section.getAttribute("class")).includes("section-visible"))
    const basicSection = await homePage.isBasicAuthSectionVisible();
    assert.ok((await basicSection.getAttribute("class")).includes("section-invisible"))
        
    resources = await homePage.getOAuthResourceOptions();
    const option = findOption("rabbit_prod", resources);
    assert.ok(option);
    assert.ok(option.selected);
    
  })
  
  it('can preselect basic auth', async function () {    
    const homePage = await login("preferred_auth_mechanism", "basic");
    
    const oauth2Section = await homePage.isOAuth2SectionVisible();    
    assert.ok((await oauth2Section.getAttribute("class")).includes("section-invisible"))
    const basicSection = await homePage.isBasicAuthSectionVisible();
    assert.ok((await basicSection.getAttribute("class")).includes("section-visible"))         
  })

  after(async function () {
    await teardown(this.driver, this, this.captureScreen);
  })
})
