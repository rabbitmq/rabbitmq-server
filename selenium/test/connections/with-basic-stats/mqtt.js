const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, doUntil } = require('../../utils')
const { openConnection, getConnectionOptions } = require('../../mqtt')

const LoginPage = require('../../pageobjects/LoginPage')
const OverviewPage = require('../../pageobjects/OverviewPage')
const ConnectionsPage = require('../../pageobjects/ConnectionsPage')
const ConnectionPage = require('../../pageobjects/ConnectionPage')

describe('MQTT connections with management stats disabled', function () {
  let driver, login, overview, connectionsPage, connectionPage, captureScreen
  let mqttClient

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    connectionsPage = new ConnectionsPage(driver)
    connectionPage = new ConnectionPage(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('management', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
  })

  it('is listed with basic info instead of the stats-disabled error', async function () {
    mqttClient = openConnection(getConnectionOptions())

    let connected = new Promise((resolve, reject) => {
      mqttClient.on('error', function (err) {
        reject(err)
        assert.fail('Mqtt connection failed due to ' + err)
      }),
      mqttClient.on('connect', function () {
        resolve('ok')
      })
    })
    assert.equal('ok', await connected)

    try {
      await overview.clickOnConnectionsTab()
      assert.ok(await overview.isPopupWarningNotDisplayed())

      const table = await doUntil(async function () {
        return connectionsPage.getConnectionsTable()
      }, function (rows) {
        return rows.length > 0
      }, 6000)

      assert.ok(table.some((row) => row.some((cell) => cell.includes('management'))))

      await connectionsPage.clickOnConnection(1)
      await connectionPage.isLoaded()

      assert.ok(await connectionPage.isSectionNotDisplayed('Channels'))
      assert.ok(await connectionPage.isSectionNotDisplayed('Sessions'))
      assert.ok(await connectionPage.isSectionNotDisplayed('Runtime Metrics'))
    } finally {
      if (mqttClient) mqttClient.end()
    }
  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
