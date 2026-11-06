const assert = require('assert')
const amqplib = require('amqplib')
const { buildDriver, goToHome, goToConnections, captureScreensFor, teardown, doUntil } = require('../../utils')
const { getAmqpUrl } = require('../../amqp')

const LoginPage = require('../../pageobjects/LoginPage')
const OverviewPage = require('../../pageobjects/OverviewPage')
const ConnectionsPage = require('../../pageobjects/ConnectionsPage')

describe('Filter AMQP 0-9-1 connections by field', function () {
  let driver
  let login
  let overview
  let connectionsPage
  let captureScreen
  let fruitConn
  let vegConn

  async function waitForHeader(predicate) {
    return doUntil(async function() {
      return connectionsPage.getPagingSectionHeaderText()
    }, predicate, 1000)
  }

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    connectionsPage = new ConnectionsPage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('guest', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }

    fruitConn = await amqplib.connect(getAmqpUrl(), {
      clientProperties: { connection_name: 'selenium-fruit', custom: 'fruit' }
    })
    vegConn = await amqplib.connect(getAmqpUrl(), {
      clientProperties: { connection_name: 'selenium-veg', custom: 'veg' }
    })
    await goToConnections(driver)
    await connectionsPage.isLoaded()
    await waitForHeader(header => header.includes('(2)'))
  })

  it('does not match client properties on the default field', async function () {
    await connectionsPage.filterConnections('', 'selenium-veg')
    await waitForHeader(header => header.includes('filtered down to 0'))
  })

  it('matches a client property on the chosen field', async function () {
    await connectionsPage.filterConnections('client_properties.custom', 'veg')
    await waitForHeader(header => header.includes('filtered down to 1'))
    let table = await connectionsPage.getConnectionsTable()
    assert.equal(table.length, 1)
    assert.ok(table[0].join(' ').includes('selenium-veg'))
  })

  it('resets a filter field the server rejects', async function () {
    await connectionsPage.filterConnections('.', 'veg')
    await doUntil(async function() {
      return connectionsPage.isPopupWarningDisplayed()
    }, displayed => displayed, 500)
    assert.ok((await connectionsPage.getPopupWarning()).includes('were reset'))
    await connectionsPage.closePopupWarning()
    await waitForHeader(header => header.includes('(2)') && !header.includes('filtered'))
  })

  after(async function () {
    if (fruitConn) await fruitConn.close()
    if (vegConn) await vegConn.close()
    // Later test cases in this directory expect these connections to be gone.
    await goToConnections(driver)
    await waitForHeader(header => header.includes('(0)'))
    await teardown(driver, this, captureScreen)
  })
})
