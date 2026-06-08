const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, doUntil } = require('../../utils')
const { openConnection, getConnectionOptions } = require('../../mqtt')

const LoginPage = require('../../pageobjects/LoginPage')
const OverviewPage = require('../../pageobjects/OverviewPage')
const ConnectionsPage = require('../../pageobjects/ConnectionsPage');


describe('List MQTT connections', function () {
  let driver
  let login
  let overview
  let captureScreen
  let mqttClient 

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    connectionsPage = new ConnectionsPage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('management', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
     
  })

  it('mqtt 5.0 connection', async function () {
    mqttClient = openConnection(getConnectionOptions())

    let connected = new Promise((resolve, reject) => {
      mqttClient.on('error', function(err) {
        reject(err)
        assert.fail("Mqtt connection failed due to " + err)        
      }),
      mqttClient.on('connect', function(err2) {
        resolve("ok")                
      })
    })    
    assert.equal("ok", await connected)
    
    try {
      await overview.clickOnConnectionsTab()
      
      let table = await doUntil(async function() {       
        return connectionsPage.getConnectionsTable()
      }, function(table) { 
        return table.length > 0
      }, 6000)
      assert.equal(table[0][5], "MQTT 5-0")

    } finally {
      if (mqttClient) mqttClient.end()
    }

  })

  after(async function () {    
    await teardown(driver, this, captureScreen)
  
  })
})
