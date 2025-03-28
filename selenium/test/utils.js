const fs = require('fs')
const XMLHttpRequest = require('xmlhttprequest').XMLHttpRequest
const fsp = fs.promises
const path = require('path')
const { By, Key, until, Builder, logging, Capabilities } = require('selenium-webdriver')
require('chromedriver')
const UAALoginPage = require('./pageobjects/UAALoginPage')
const KeycloakLoginPage = require('./pageobjects/KeycloakLoginPage')
const assert = require('assert')

const uaaUrl = process.env.UAA_URL || 'http://localhost:8080'
const baseUrl = randomly_pick_baseurl(process.env.RABBITMQ_URL) || 'http://localhost:15672/'
const hostname = process.env.RABBITMQ_HOSTNAME || 'localhost'
const runLocal = String(process.env.RUN_LOCAL).toLowerCase() != 'false'
const seleniumUrl = process.env.SELENIUM_URL || 'http://selenium:4444'
const screenshotsDir = process.env.SCREENSHOTS_DIR || '/screens'
const profiles = process.env.PROFILES || ''

function randomly_pick_baseurl(baseUrl) {
    urls = baseUrl.split(",")
    return urls[getRandomInt(urls.length)]
}
function getRandomInt(max) {
  return Math.floor(Math.random() * max);
}
class CaptureScreenshot {
  driver
  test
  constructor (webdriver, test) {
    this.driver = webdriver
    this.test = test
  }

  async shot (name) {
    const image = await driver.takeScreenshot()
    const screenshotsSubDir = path.join(screenshotsDir, this.test)
    if (!fs.existsSync(screenshotsSubDir)) {
      await fsp.mkdir(screenshotsSubDir)
    }
    const dest = path.join(screenshotsSubDir, name + '.png')
    await fsp.writeFile(dest, image, 'base64')
  }
}

module.exports = {
  log: (message) => {
    console.log(new Date() + " " + message)
  },

  hasProfile: (profile) => {
    return profiles.includes(profile)
  },

  buildDriver: (caps) => {
    builder = new Builder()
    if (!runLocal) {
      builder = builder.usingServer(seleniumUrl)
    }
    let chromeCapabilities = Capabilities.chrome();
    chromeCapabilities.setAcceptInsecureCerts(true);
    chromeCapabilities.set('goog:chromeOptions', {
      args: [
          "--lang=en",
          "--disable-search-engine-choice-screen"
      ]
    });
    driver = builder
      .forBrowser('chrome')
      .withCapabilities(chromeCapabilities)
      .build()
    driver.manage().setTimeouts( { pageLoad: 35000 } )
    return driver
  },

  getURLForProtocol: (protocol) => {

    switch(protocol) {
      case "amqp": return "amqp://" + hostname
      default: throw new Error("Unknown prootocol " + protocol)
    }
  },

  goToHome: (driver) => {
    return driver.get(baseUrl)
  },

  goToLogin: (driver, token) => {
    return driver.get(baseUrl + '#/login?access_token=' + token)
  },

  goToExchanges: (driver) => {
    return driver.get(baseUrl + '#/exchanges')
  },

  goTo: (driver, address) => {
    return driver.get(address)
  },

  delay: async (msec, ref) => {
    return new Promise(resolve => {
      setTimeout(resolve, msec, ref)
    })
  },

  captureScreensFor: (driver, test) => {
    return new CaptureScreenshot(driver, require('path').basename(test))
  },

  idpLoginPage: (driver, preferredIdp) => {
    if (!preferredIdp) {
      if (process.env.PROFILES.includes("uaa")) {
        preferredIdp = "uaa"
      } else if (process.env.PROFILES.includes("keycloak")) {
        preferredIdp = "keycloak"
      } else {
        throw new Error("Missing uaa or keycloak profiles")
      }
    }
    switch(preferredIdp) {
      case "uaa": return new UAALoginPage(driver)
      case "keycloak": return new KeycloakLoginPage(driver)
      default: new Error("Unsupported ipd " + preferredIdp)
    }
  },
  openIdConfiguration: (url) => {
    const req = new XMLHttpRequest()
    req.open('GET', url + "/.well-known/openid-configuration", false)
    req.send()
    if (req.status == 200) return JSON.parse(req.responseText)
    else {
      console.error(JSON.stringify(req.statusText) + ", " + req.responseText)
      throw new Error(req.responseText)
    }
  },

  rest_get: (url, access_token) => {
    const req = new XMLHttpRequest()
    req.open('GET', url, false)
    req.setRequestHeader('Accept', 'application/json')
    req.setRequestHeader('Authorization', 'Bearer ' + access_token)
    req.send()
    if (req.status == 200) return JSON.parse(req.responseText)
    else {
      console.error(JSON.stringify(req.statusText) + ", " + req.responseText)
      throw new Error(req.responseText)
    }
  },

  tokenFor: (client_id, client_secret, url = uaaUrl) => {
    const req = new XMLHttpRequest()
    const params = 'client_id=' + client_id +
      '&client_secret=' + client_secret +
      '&grant_type=client_credentials' +
      '&token_format=jwt' +
      '&response_type=token'
    req.open('POST', url, false)
    req.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded')
    req.setRequestHeader('Accept', 'application/json')
    req.send(params)
    if (req.status == 200) return JSON.parse(req.responseText).access_token
    else {
      console.error(JSON.stringify(req.statusText) + ", " + req.responseText)
      throw new Error(req.responseText)
    }
  },

  assertAllOptions: (expectedOptions, actualOptions) => {
    assert.equal(expectedOptions.length, actualOptions.length)
    for (let i = 0; i < expectedOptions.length; i++) {
      assert.ok(actualOptions.find((actualOption) =>
        actualOption.value == expectedOptions[i].value
          && actualOption.text == expectedOptions[i].text))
    }
  },

  teardown: async (driver, test, captureScreen = null) => {
    driver.manage().logs().get(logging.Type.BROWSER).then(function(entries) {
        entries.forEach(function(entry) {
          console.log('[%s] %s', entry.level.name, entry.message);
        })
     })
    if (test.currentTest) {
      if (test.currentTest.isPassed()) {
        driver.executeScript('lambda-status=passed')
      } else {
        if (captureScreen != null) await captureScreen.shot('after-failed')
        driver.executeScript('lambda-status=failed')
      }
    }
    await driver.quit()
  }

}
