const { By, Key, until, Builder } = require('selenium-webdriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../utils')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const ExchangesPage = require('../pageobjects/ExchangesPage')
const ExchangePage = require('../pageobjects/ExchangePage')

const DISABLE_METRICS = process.env.DISABLE_METRICS || false

/** Default exchanges on `/` and `other` vhosts; extra rows from plugins etc. are allowed */
const EXPECTED_DEFAULT_EXCHANGE_ROWS = [
  ['/', '(AMQP default)', 'direct'],
  ['/', 'amq.direct', 'direct'],
  ['/', 'amq.fanout', 'fanout'],
  ['/', 'amq.headers', 'headers'],
  ['/', 'amq.match', 'headers'],
  ['/', 'amq.rabbitmq.trace', 'topic'],
  ['/', 'amq.topic', 'topic'],
  ['other', '(AMQP default)', 'direct'],
  ['other', 'amq.direct', 'direct'],
  ['other', 'amq.fanout', 'fanout'],
  ['other', 'amq.headers', 'headers'],
  ['other', 'amq.match', 'headers'],
  ['other', 'amq.rabbitmq.trace', 'topic'],
  ['other', 'amq.topic', 'topic']
]

describe('Exchange management', function () {
  let driver
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
    await overview.selectRefreshOption("Do not refresh")
    await overview.clickOnExchangesTab()
  })

  it('display summary of exchanges', async function () {
    const header = await exchanges.getPagingSectionHeaderText()
    const match = header.match(/^All exchanges \((\d+)\)$/)
    assert.ok(
      match,
      `expected paging header like "All exchanges (N)", got ${JSON.stringify(header)}`
    )
    const count = parseInt(match[1], 10)
    assert.ok(
      count >= EXPECTED_DEFAULT_EXCHANGE_ROWS.length,
      `expected at least ${EXPECTED_DEFAULT_EXCHANGE_ROWS.length} exchanges, got ${count}`
    )
  })

  it('list all default exchanges', async function () {
    const actualTable = await exchanges.getExchangesTable(3)

    function rowMatches (actual, expected) {
      return (
        actual.length >= expected.length &&
        expected.every((cell, i) => actual[i] === cell)
      )
    }

    for (const expectedRow of EXPECTED_DEFAULT_EXCHANGE_ROWS) {
      const found = actualTable.some((row) => rowMatches(row, expectedRow))
      assert.ok(
        found,
        `expected exchange row not in table: ${JSON.stringify(expectedRow)}`
      )
    }
    assert.ok(
      actualTable.length >= EXPECTED_DEFAULT_EXCHANGE_ROWS.length,
      `expected at least ${EXPECTED_DEFAULT_EXCHANGE_ROWS.length} exchange rows, got ${actualTable.length}`
    )
  })

  it('view one exchange', async function () {
    await exchanges.clickOnExchange("%2F", "amq.fanout")
    await exchange.isLoaded()
    assert.equal("amq.fanout", await exchange.getName())
  })
/*
  it('exchange selectable columns', async function () {  
    await overview.clickOnExchangesTab()
    await doUntil(async function() { return exchanges.getExchangesTable() },
      function(table) { 
        return table.length > 0
    })

    log("Opening selectable columns popup...")
    await exchanges.clickOnSelectTableColumns()
    log("Getting all selectable dolumns ...")
    let table = await exchanges.getSelectableTableColumns()
    
    log("Asserting selectable dolumns ...")
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
    
    if (!DISABLE_METRICS) {
      assert.equal(table.length, 2)

      let messageRatesGroup = { 
        "name" : "Message rates:",
        "columns": [
          {"name:":"rate in","id":"checkbox-exchanges-rate-in"},
          {"name:":"rate out","id":"checkbox-exchanges-rate-out"}
        ]
      }
      assert.equal(JSON.stringify(table[1]), JSON.stringify(messageRatesGroup))
    }
      
  })
*/

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
