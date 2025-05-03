const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, doWhile, log, delay } = require('../utils')
const { getManagementUrl, createVhost, deleteVhost } = require('../mgt-api')

const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')
const AdminTab = require('../pageobjects/AdminTab')
const VhostsAdminTab = require('../pageobjects/VhostsAdminTab')
const VhostAdminTab = require('../pageobjects/VhostAdminTab')

describe('Virtual Hosts in Admin tab', function () {
  let login
  let overview
  let captureScreen

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    adminTab = new AdminTab(driver)
    vhostsTab = new VhostsAdminTab(driver)
    vhostTab = new VhostAdminTab(driver)
    captureScreen = captureScreensFor(driver, __filename)

    await login.login('guest', 'guest')
    if (!await overview.isLoaded()) {
      throw new Error('Failed to login')
    }
    await overview.selectRefreshOption("Do not refresh")
  })

  it('find default vhost', async function () {
    await overview.clickOnAdminTab()
    await adminTab.clickOnVhosts()
    assert.equal(true, await vhostsTab.hasVhosts("/"))
  })
  it('find default vhost and view it', async function () {
    await overview.clickOnOverviewTab()
    await overview.clickOnAdminTab()
    await adminTab.clickOnVhosts()
    await vhostsTab.clickOnVhost(await vhostsTab.searchForVhosts("/"), "/")
    if (!await vhostTab.isLoaded()) {
      throw new Error('Failed to load vhost')
    }
    assert.equal("/", await vhostTab.getName())
  })
    
  it('vhost selectable columns', async function () {  
    await overview.clickOnOverviewTab()
    await overview.clickOnAdminTab()
    await adminTab.clickOnVhosts()
    await vhostsTab.searchForVhosts("/")
    await doWhile(async function() { return vhostsTab.getVhostsTable() },
      function(table) { 
        return table.length>0
      })

    await vhostsTab.clickOnSelectTableColumns()
    let table = await vhostsTab.getSelectableTableColumns()
    
    assert.equal(4, table.length)
    let overviewGroup = { 
        "name" : "Overview:",
        "columns": [
          {"name:":"Default queue type","id":"checkbox-vhosts-default-queue-type"},
          {"name:":"Cluster state","id":"checkbox-vhosts-cluster-state"},
          {"name:":"Description","id":"checkbox-vhosts-description"},
          {"name:":"Tags","id":"checkbox-vhosts-tags"}
        ]
      }
    assert.equal(JSON.stringify(table[0]), JSON.stringify(overviewGroup))
    let messagesGroup = { 
      "name" : "Messages:",
      "columns": [
        {"name:":"Ready","id":"checkbox-vhosts-msgs-ready"},
        {"name:":"Unacknowledged","id":"checkbox-vhosts-msgs-unacked"},
        {"name:":"Total","id":"checkbox-vhosts-msgs-total"}
      ]
    }
    assert.equal(JSON.stringify(table[1]), JSON.stringify(messagesGroup))
    let networkGroup = { 
      "name" : "Network:",
      "columns": [
        {"name:":"From client","id":"checkbox-vhosts-from_client"},
        {"name:":"To client","id":"checkbox-vhosts-to_client"}
      ]
    }
    assert.equal(JSON.stringify(table[2]), JSON.stringify(networkGroup))
    let messageRatesGroup = { 
      "name" : "Message rates:",
      "columns": [
        {"name:":"publish","id":"checkbox-vhosts-rate-publish"},
        {"name:":"deliver / get","id":"checkbox-vhosts-rate-deliver"}
      ]
    }
    assert.equal(JSON.stringify(table[3]), JSON.stringify(messageRatesGroup))
  
  })
  
  describe('given there is a new virtualhost with a tag', async function() {
    let vhost = "test_" + Math.floor(Math.random() * 1000)
    before(async function() {
      log("Creating vhost")
      createVhost(getManagementUrl(), vhost, "selenium", "selenium-tag")
      await overview.clickOnOverviewTab()
      await overview.clickOnAdminTab()
      await adminTab.clickOnVhosts()
    })
    it('vhost is listed with tag', async function () {  
      log("Searching for vhost " + vhost)
      await vhostsTab.searchForVhosts(vhost)
      await doWhile(async function() { return vhostsTab.getVhostsTable()},
      function(table) { 
        log("table: "+ JSON.stringify(table) + " table[0][0]:" + table[0][0])
        return table.length==1 && table[0][0].localeCompare(vhost) == 0
      })
      log("Found vhost " + vhost)
      await vhostsTab.selectTableColumnsById(["checkbox-vhosts-tags"])
      
      await doWhile(async function() { return vhostsTab.getVhostsTable() },
      function(table) { 
        return table.length==1 && table[0][3].localeCompare("selenium-tag") == 0
      })

    })
    after(async function () {
      log("Deleting vhost")
      deleteVhost(getManagementUrl(), vhost)
    })

  })
  

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
