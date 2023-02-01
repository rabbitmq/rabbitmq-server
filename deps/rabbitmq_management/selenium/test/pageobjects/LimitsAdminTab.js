const { By, Key, until, Builder } = require('selenium-webdriver')

const AdminTab = require('./AdminTab')

const SELECTED_LIMITS_ON_RHM = By.css('div#rhs ul li a[href="#/limits"]')

const VIRTUAL_HOST_LIMITS_SECTION = By.css('div#main div#virtual-host-limits')
const USER_LIMITS_SECTION = By.css('div#main div#user-limits')

<<<<<<< HEAD
<<<<<<< HEAD
const VIRTUAL_HOST_LIMITS_TABLE_ROWS = By.css('div#main div#virtual-host-limits table.list tbody tr')
const USER_LIMITS_TABLE_ROWS = By.css('div#main div#user-limits table.list tbody tr')

=======
>>>>>>> 5ad72497a3 (Test display limits when there are none)
=======
const VIRTUAL_HOST_LIMITS_TABLE_ROWS = By.css('div#main div#virtual-host-limits table.list tbody tr')
const USER_LIMITS_TABLE_ROWS = By.css('div#main div#user-limits table.list tbody tr')

>>>>>>> 166faa3629 (Test when there are zero limits)
module.exports = class LimitsAdminTab extends AdminTab {
  async isLoaded () {
    await this.waitForDisplayed(SELECTED_LIMITS_ON_RHM)
  }

  async list_virtual_host_limits() {
    await this.click(VIRTUAL_HOST_LIMITS_SECTION)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 166faa3629 (Test when there are zero limits)
    try
    {
      rows = driver.findElements(VIRTUAL_HOST_LIMITS_TABLE_ROWS)
      return rows
    } catch (NoSuchElement) {
      return Promise.resolve([])
    }
  }
  async list_user_limits() {
    await this.click(USER_LIMITS_SECTION)
    try
    {
      rows = driver.findElements(VIRTUAL_HOST_LIMITS_TABLE_ROWS)
      return rows
    } catch (NoSuchElement) {
      return Promise.resolve([])
    }
<<<<<<< HEAD
=======
  }
  async list_user_limits() {
    await this.click(USER_LIMITS_SECTION)
>>>>>>> 5ad72497a3 (Test display limits when there are none)
=======
>>>>>>> 166faa3629 (Test when there are zero limits)
  }

}
