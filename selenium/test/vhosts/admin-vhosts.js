const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, doWhile, log } = require('../utils')
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

  })

  it('find default vhost', async function () {
    await overview.clickOnAdminTab()
    await adminTab.clickOnVhosts()
    assert.equal(true, await vhostsTab.hasVhosts("/"))
  })
  it('find default vhost and view it', async function () {
    await overview.clickOnAdminTab()
    await adminTab.clickOnVhosts()
    await vhostsTab.clickOnVhost(await vhostsTab.searchForVhosts("/"), "/")
    if (!await vhostTab.isLoaded()) {
      throw new Error('Failed to load vhost')
    }
    assert.equal("/", await vhostTab.getName())
  })

  describe('given there is a new virtualhost with a tag', async function() {
    let vhost = "test_" + Math.floor(Math.random() * 1000)
    before(async function() {
      createVhost(getManagementUrl(), vhost, "selenium", "selenium-tag")
      await overview.clickOnAdminTab()
      await adminTab.clickOnVhosts()      
    })
    it('vhost is listed', async function () {      
      await vhostsTab.searchForVhosts(vhost)
      await doWhile(async function() {
        return vhostsTab.getVhostsTable()
      }, function(table) {
        return table.length > 0 && vhost.localeCompare(table[0][0])        
      })
      await vhostsTab.clickOnSelectColumns()
      await vhostsTab.getSelectableTableColumns()
    })
    after(async function () {
      deleteVhost(getManagementUrl(), vhost)
    })

  })

  after(async function () {
    await teardown(driver, this, captureScreen)
  })
})
