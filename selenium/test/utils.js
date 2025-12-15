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
const debug = process.env.SELENIUM_DEBUG || false

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
    const image = await this.driver.takeScreenshot()
    const screenshotsSubDir = path.join(screenshotsDir, this.test)
    if (!fs.existsSync(screenshotsSubDir)) {
      await fsp.mkdir(screenshotsSubDir)
    }    
    const dest = path.join(screenshotsSubDir, name + '.png')
    console.log("screenshot saved to " + dest)
    await fsp.writeFile(dest, image, 'base64')
  }
}

module.exports = {
  log: (message) => {
    if (debug) console.log(new Date() + " " + message)
  },
  error: (message) => {
    console.error(new Date() + " " + message)
  },
  hasProfile: (profile) => {
    return profiles.includes(profile)
  },

  buildDriver: (url = baseUrl) => {
    builder = new Builder()
    if (!runLocal) {
      builder = builder.usingServer(seleniumUrl)
    }
    let chromeCapabilities = Capabilities.chrome();
    const options = new chrome.Options()
    chromeCapabilities.setAcceptInsecureCerts(true);  
    let seleniumArgs = [
      "--window-size=1920,1080",
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
    if (!runLocal) {
      seleniumArgs.push("--headless=new")
    }
    chromeCapabilities.set('goog:chromeOptions', {
      excludeSwitches: [ // disable info bar
        'enable-automation',
      ],
      prefs: {
        'profile.password_manager_enabled' : false      
      },
      args: seleniumArgs
    });
    let driver = builder
      .forBrowser('chrome')
      //.setChromeOptions(options.excludeSwitches("disable-popup-blocking", "enable-automation"))
      .withCapabilities(chromeCapabilities)
      .build()
    driver.manage().setTimeouts( { pageLoad: 35000 } )
    return {
      "driver": driver, 
      "baseUrl": url
    }
  },
  updateDriver: (d, url) => {
    return {
      "driver" : d.driver, 
      "baseUrl" : url
    }
  },
  getURLForProtocol: (protocol) => {

    switch(protocol) {
      case "amqp": return "amqp://" + hostname
      default: throw new Error("Unknown prootocol " + protocol)
    }
  },

  goToHome: (d) => {
    module.exports.log("goToHome on " + d.baseUrl)
    return d.driver.get(d.baseUrl)
  },

  /**
   * For instance, 
   * goToLogin(d, access_token, myAccessToken)
   * or 
   * goToLogin(d, preferred_auth_mechanism, "oauth2:my-resource")
   */
  goToLogin: (d, ...keyValuePairs) => {
    const params = [];
    for (let i = 0; i < keyValuePairs.length; i += 2) {
        const key = keyValuePairs[i];
        const value = keyValuePairs[i + 1];
        
        if (key !== undefined) {
            // URL-encode both key and value
            const encodedKey = encodeURIComponent(key);
            const encodedValue = encodeURIComponent(value || '');
            params.push(`${encodedKey}=${encodedValue}`);
        }
    }    
    // Build query string: "key1=value1&key2=value2"
    const queryString = params.join('&');
    
    const url = d.baseUrl + '/login?' + queryString;
    console.log("Navigating to " + url);
    return d.driver.get(url);
  },

  goToConnections: (d) => {
    return d.driver.get(d.baseUrl + '#/connections')
  },

  goToExchanges: (d) => {
    return d.driver.get(d.baseUrl + '#/exchanges')
  },
    
  goToQueues: (d) => {
    return d.driver.get(d.baseUrl + '#/queues')
  },
    
  goToQueue(d, vhost, queue) {
    return d.driver.get(d.baseUrl + '#/queues/' + encodeURIComponent(vhost) + '/' + encodeURIComponent(queue))
  },

  delay: async (msec, ref) => {
    return new Promise(resolve => {
      setTimeout(resolve, msec, ref)
    })
  },

  captureScreensFor: (d, test) => {
    return new CaptureScreenshot(d.driver, require('path').basename(test))
  },

  doUntil: async (doCallback, booleanCallback, delayMs = 1000, message = "doUntil failed") => {
    let done = false 
    let attempts = 10
    let ret
    do {
      try {
        module.exports.log("Calling doCallback (attempts:" + attempts + ") ... ")
        ret = await doCallback()
        module.exports.log("Calling booleanCallback (attempts:" + attempts 
          + ") with arg " + JSON.stringify(ret) + " ... ")
        done =  booleanCallback(ret)
      }catch(error) {
        module.exports.error("Caught " + error + " on doUntil callback...")
        
      }finally {
        if (!done) {
          module.exports.log("Waiting until next attempt")
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
  retry: async (doCallback, booleanCallback, delayMs = 1000, message = "retry failed") => {
    let done = false 
    let attempts = 10
    let ret
    do {
      try {
        module.exports.log("Calling doCallback (attempts:" + attempts + ") ... ")
        ret = doCallback()
        module.exports.log("Calling booleanCallback (attempts:" + attempts 
          + ") with arg " + JSON.stringify(ret) + " ... ")
        done =  booleanCallback(ret)
      }catch(error) {
        module.exports.error("Caught " + error + " on retry callback...")
        
      }finally {
        if (!done) {
          module.exports.log("Waiting until next attempt")
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

  idpLoginPage: (d, preferredIdp) => {
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
      case "uaa": return new UAALoginPage(d)
      case "keycloak": return new KeycloakLoginPage(d)
      default: new Error("Unsupported ipd " + preferredIdp)
    }
  },
  openIdConfiguration: (url) => {
    const req = new XMLHttpRequest()
    req.open('GET', url + "/.well-known/openid-configuration", false)
    req.send()
    if (req.status == 200) return JSON.parse(req.responseText)
    else {
      module.exports.error(req.responseText)
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
      module.exports.error(req.responseText)
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
  findOption: (value, options) => {
    for (let i = 0; i < options.length; i++) {
      if (options[i].value === value) return options[i];
    }
    return undefined;
  },

  teardown: async (d, test, captureScreen = null) => {
    
    driver = d.driver
    driver.manage().logs().get(logging.Type.BROWSER).then(function(entries) {
        entries.forEach(function(entry) {
          module.exports.log('[%s] %s', entry.level.name, entry.message);
        })
     })
    if (test.currentTest) {
      if (test.currentTest.isPassed()) {
        driver.executeScript('lambda-status=passed')
      } else {        
        if (captureScreen != null) {
          console.log("Teardown failed . capture...");
          await captureScreen.shot('after-failed');
        }
        driver.executeScript('lambda-status=failed')
      }
    }
    await driver.quit()
  },

  findTableRow: (table, booleanCallback) => {
    if (!table) return false

    let i = 0
    while (i < table.length && !booleanCallback(table[i])) i++;      
    return i < table.length ? table[i] : undefined
  }

}
