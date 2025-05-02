const fs = require('fs')
const XMLHttpRequest = require('xmlhttprequest').XMLHttpRequest
const fsp = fs.promises
const path = require('path')
const { By, Key, until, Builder, logging, Capabilities } = require('selenium-webdriver')
const proxy = require('selenium-webdriver/proxy')
require('chromedriver')
var chrome = require("selenium-webdriver/chrome");
const UAALoginPage = require('./pageobjects/UAALoginPage')
const KeycloakLoginPage = require('./pageobjects/KeycloakLoginPage')
const assert = require('assert')

const runLocal = String(process.env.RUN_LOCAL).toLowerCase() != 'false'
const uaaUrl = process.env.UAA_URL || 'http://localhost:8080'
const baseUrl = randomly_pick_baseurl(process.env.RABBITMQ_URL) || 'http://localhost:15672/'
const hostname = process.env.RABBITMQ_HOSTNAME || 'localhost'
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
  error: (message) => {
    console.error(new Date() + " " + message)
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
    const options = new chrome.Options()
    chromeCapabilities.setAcceptInsecureCerts(true);  
    chromeCapabilities.set('goog:chromeOptions', {
      excludeSwitches: [ // disable info bar
        'enable-automation',
      ],
      prefs: {
        'profile.password_manager_enabled' : false      
      },
      args: [
          "--enable-automation",
          "guest",
          "disable-infobars",
          "--disable-notifications",
          "--lang=en",
          "--disable-search-engine-choice-screen",
          "disable-popup-blocking",
          "--credentials_enable_service=false",
          "profile.password_manager_enabled=false",
          "profile.reduce-security-for-testing",
          "profile.managed_default_content_settings.popups=1",
          "profile.managed_default_content_settings.notifications.popups=1",
          "profile.password_manager_leak_detection=false"
      ]
    });
    driver = builder
      .forBrowser('chrome')
      //.setChromeOptions(options.excludeSwitches("disable-popup-blocking", "enable-automation"))
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

  doWhile: async (doCallback, booleanCallback, delayMs = 1000, message = "doWhile failed") => {
    let done = false 
    let attempts = 10
    let ret
    do {
      try {
        //console.log("Calling doCallback (attempts:" + attempts + ") ... ")
        ret = await doCallback()
        //console.log("Calling booleanCallback (attempts:" + attempts + ") with arg " + ret + " ... ")
        done =  booleanCallback(ret)
      }catch(error) {
        console.log("Caught " + error + " on doWhile callback...")
        
      }finally {
        if (!done) {
          //console.log("Waiting until next attempt")
          await module.exports.delay(delayMs)
        }
      }     
      attempts--
    } while (attempts > 0 && !done)
    if (!done) {
      throw new Error(message)
    }else {
      return ret
    }
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
      console.error(req.responseText)
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
      console.error(req.responseText)
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
