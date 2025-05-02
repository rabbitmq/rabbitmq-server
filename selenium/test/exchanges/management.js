const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, doWhile } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const ExchangesPage = require('../pageobjects/ExchangesPage')
const ExchangePage = require('../pageobjects/ExchangePage')

describe('Exchange management', function () {
  let login
  let exchanges
  let exchange
  let overview
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    exchanges = new ExchangesPage(driver)
    exchange = new ExchangePage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('guest', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    overview.clickOnExchangesTab()
  })

  it('display summary of exchanges', async function () {
    assert.equal("All exchanges (14)", await exchanges.getPagingSectionHeaderText())
  })

  it('list all default exchanges', async function () {
    let actual_table = await exchanges.getExchangesTable(3)
    
    let expected_table = [
      ["/", "(AMQP default)", "direct"],
      ["/", "amq.direct", "direct"],
      ["/", "amq.fanout", "fanout"],
      ["/", "amq.headers", "headers"],
      ["/", "amq.match", "headers"],
      ["/", "amq.rabbitmq.trace", "topic"],
      ["/", "amq.topic", "topic"],
   
      ["other", "(AMQP default)", "direct"],
      ["other", "amq.direct", "direct"],
      ["other", "amq.fanout", "fanout"],
      ["other", "amq.headers", "headers"],
      ["other", "amq.match", "headers"],
      ["other", "amq.rabbitmq.trace", "topic"],
      ["other", "amq.topic", "topic"]
    ]
    
    console.log("e :" + actual_table)
    assert.deepEqual(actual_table, expected_table)
  })

  it('view one exchange', async function () {
    await exchanges.clickOnExchange("%2F", "amq.fanout")
    await exchange.isLoaded()
    assert.equal("amq.fanout", await exchange.getName())
  })

  it('exchange selectable columns', async function () {  
    await overview.clickOnOverviewTab()
    await overview.clickOnExchangesTab()
    await doWhile(async function() { return exchanges.getExchangesTable() },
      function(table) { 
        return table.length > 0
    })

    await exchanges.clickOnSelectTableColumns()
    let table = await exchanges.getSelectableTableColumns()
    
    assert.equal(2, table.length)
    let overviewGroup = { 
        "name" : "Overview:",
        "columns": [
          {"name:":"Type","id":"checkbox-exchanges-type"},
          {"name:":"Features (with policy)","id":"checkbox-exchanges-features"},
          {"name:":"Features (no policy)","id":"checkbox-exchanges-features_no_policy"},
          {"name:":"Policy","id":"checkbox-exchanges-policy"}
        ]
    }    
    assert.equal(JSON.stringify(table[0]), JSON.stringify(overviewGroup))
    
    let messageRatesGroup = { 
      "name" : "Message rates:",
      "columns": [
        {"name:":"rate in","id":"checkbox-exchanges-rate-in"},
        {"name:":"rate out","id":"checkbox-exchanges-rate-out"}
      ]
    }
    assert.equal(JSON.stringify(table[1]), JSON.stringify(messageRatesGroup))
      
  })


  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
