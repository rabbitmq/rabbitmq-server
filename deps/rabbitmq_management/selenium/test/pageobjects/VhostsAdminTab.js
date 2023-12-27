const { By, Key, until, Builder } = require('selenium-webdriver')

const AdminTab = require('./AdminTab')

const SELECTED_VHOSTS_ON_RHM = By.css('div#rhs ul li a[href="#/vhosts"]')
const FILTER_VHOST = By.css('div#main div.filter input#filter')
const CHECKBOX_REGEX = By.css('div#main div.filter input#filter-regex-mode')

const VHOSTS_TABLE_ROWS = By.css('div#main table.list tbody tr')

module.exports = class VhostsAdminTab extends AdminTab {
  async isLoaded () {
    await this.waitForDisplayed(SELECTED_VHOSTS_ON_RHM)
  }
  async searchForVhosts(vhost, regex = false) {
    await this.sendKeys(FILTER_VHOST, vhost)
    await this.sendKeys(FILTER_VHOST, Key.RETURN)
    if (regex) {
      await this.click(CHECKBOX_REGEX)
    }
    await this.driver.sleep(250)
    return this.waitForDisplayed(VHOSTS_TABLE_ROWS)
  }
  async hasVhosts(vhost, regex = false) {
    return await this.searchForVhosts(vhost, regex) != undefined
  }
  async clickOnVhost(vhost_rows, vhost) {
     let links = await vhost_rows.findElements(By.css("td a"))
     for (let link of links) {
       let text = await link.getText()
       if ( text === "/" ) return link.click()
     }
     throw "Vhost " + vhost + " not found"
  }

}
