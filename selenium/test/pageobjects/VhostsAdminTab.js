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
    //await this.sendKeys(FILTER_VHOST, Key.RETURN)
    if (regex) {
      await this.click(CHECKBOX_REGEX)
    }
    await this.driver.sleep(250)
    await this.waitForDisplayed(VHOSTS_TABLE_ROWS)
    return this.driver.findElements(VHOSTS_TABLE_ROWS)
  }
  async hasVhosts(vhost, regex = false) {
    return await this.searchForVhosts(vhost, regex) != undefined
  }
  
  async clickOnVhost(vhost) {
    return this.retryOnStale(async () => {
      const rows = await this.driver.findElements(VHOSTS_TABLE_ROWS)
      for (let row of rows) {
        const link = await row.findElement(By.css("td a"))
        let text = await link.getText()
        if ( text === vhost ) return link.click()
      }
      throw new Error("Vhost " + vhost + " not found")
    })
  }
  async getVhostsTable(firstNColumns) {
    return this.getTable(TABLE_SECTION, firstNColumns)
  }

}
