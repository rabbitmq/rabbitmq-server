const { By, Key, until, Builder } = require('selenium-webdriver')

const AdminTab = require('./AdminTab')

const MAIN_SECTION = By.css('div#main div#vhosts.section')

const SELECTED_VHOSTS_ON_RHM = By.css('div#rhs ul li a[href="#/vhosts"]')
const FILTER_VHOST = By.css('div#main div.filter input#filter')
const CHECKBOX_REGEX = By.css('div#main div.filter input#filter-regex-mode')

const VHOSTS_TABLE_ROWS = By.css('div#main div#vhosts table.list tbody tr')
const TABLE_SECTION = By.css('div#main div#vhosts.section table.list')

module.exports = class VhostsAdminTab extends AdminTab {
  async isLoaded () {
    return this.waitForDisplayed(MAIN_SECTION)
  }
  async searchForVhosts(vhost, regex = false) {
    await this.sendKeys(FILTER_VHOST, vhost)
    if (regex) {
      await this.click(CHECKBOX_REGEX)
    }
    // Allow extra time for the AJAX filter response to replace the table DOM
//    await this.driver.sleep(500)
    return this.retryOnStale(async () => {
      return this.driver.findElements(VHOSTS_TABLE_ROWS)
    })
  }

  async hasVhosts(vhost, regex = false, timeout = 5000) {
    // searchForVhosts waits a fixed interval for the AJAX filter response, so a
    // single call can observe the table before it has been replaced. Retry until
    // a row shows up or the timeout expires, and only then report absence.
    const deadline = Date.now() + timeout
    do {
      const rows = await this.searchForVhosts(vhost, regex)
      if (rows.length > 0) {
        return true
      }
    } while (Date.now() < deadline)
    return false
  }

  async clickOnVhost(vhost) {
    return this.retryOnStale(async () => {
      const rows = await this.driver.findElements(VHOSTS_TABLE_ROWS)
      for (const row of rows) {
        const link = await row.findElement(By.css("td a"))
        const text = await link.getText()
        if (text === vhost) return link.click()
      }
      throw new Error("Vhost " + vhost + " not found")
    })
  }
  async getVhostsTable(firstNColumns) {
    return this.getTable(TABLE_SECTION, firstNColumns)
  }

}
