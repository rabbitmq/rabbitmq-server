const fs = require('fs')
const XMLHttpRequest = require('xmlhttprequest').XMLHttpRequest
const fsp = fs.promises
const path = require('path')
const { By, Key, until, Builder, logging } = require('selenium-webdriver')
require('chromedriver')

const uaaUrl = process.env.UAA_URL || 'http://localhost:8080'
const baseUrl = process.env.RABBITMQ_URL || 'http://localhost:15672'
const runLocal = String(process.env.RUN_LOCAL).toLowerCase() != 'false'
const seleniumUrl = process.env.SELENIUM_URL || 'http://selenium:4444'
const screenshotsDir = process.env.SCREENSHOTS_DIR || '/screens'

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
  buildDriver: (caps) => {
    builder = new Builder()
    if (!runLocal) {
      builder = builder.usingServer(seleniumUrl)
    }
    return builder.forBrowser('chrome').build()
  },

  goToHome: (driver) => {
    return driver.get(baseUrl)
  },

  goToLogin: (driver, token) => {
    return driver.get(baseUrl + '/#/login?access_token=' + token)
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

  tokenFor: (client_id, client_secret) => {
    const req = new XMLHttpRequest()
    const url = uaaUrl + '/oauth/token'
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
