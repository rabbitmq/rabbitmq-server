// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

const assert = require('assert')
const { By, Key, until } = require('selenium-webdriver')
const { buildDriver, goToHome, captureScreensFor, teardown } = require('../utils')
const LoginPage = require('../pageobjects/LoginPage')
const OverviewPage = require('../pageobjects/OverviewPage')

describe('Bunny Hop', function () {
  this.timeout(90000)
  let browser, driver, root, captureScreen
  const state = () => driver.executeScript('return document.body.dataset.state')
  const waitState = value => driver.wait(async () => await state() === value, 15000, `Expected ${value}`)
  const action = async () => (await driver.findElement(By.id('action'))).click()
  const scores = () => driver.executeScript(`return ['distance','hops'].map(id => document.getElementById(id).textContent)`)
  const key = async value => driver.actions().sendKeys(value).perform()
  const open = async () => {
    await driver.get(new URL('bunny-hop', root).href)
    await waitState('ready')
  }

  before(async function () {
    browser = buildDriver()
    driver = browser.driver
    captureScreen = captureScreensFor(browser, __filename)
    await goToHome(browser)
    await new LoginPage(browser).login('guest', 'guest')
    assert.ok(await new OverviewPage(browser).isLoaded())
    const current = new URL(await driver.getCurrentUrl())
    current.hash = ''
    root = current.href
  })

  after(async function () { await teardown(browser, this, captureScreen) })

  it('loads only local assets, supports all viewport sizes, and starts with zero counters', async function () {
    for (const [width, height] of [[360, 640], [768, 1024], [1920, 1080]]) {
      await driver.manage().window().setRect({ width, height })
      await open()
      assert.deepEqual(await scores(), ['0', '0'])
      assert.ok(await driver.executeScript('return document.documentElement.scrollWidth <= innerWidth'))
      assert.ok(await driver.executeScript(`return performance.getEntriesByType('resource').every(e => new URL(e.name).origin === location.origin)`))
      await key(Key.TAB)
      assert.ok(await driver.executeScript('return document.activeElement !== document.body'))
    }
  })

  it('jumps with keyboard and pointer and pauses without progress until explicitly resumed', async function () {
    await open()
    await action()
    await driver.wait(async () => Number((await scores())[0]) >= 930, 6000)
    await key(Key.SPACE)
    await driver.wait(async () => Number((await scores())[1]) === 1, 3000)
    await driver.executeScript(`document.getElementById('course').dispatchEvent(new PointerEvent('pointerdown', {isPrimary:true, button:0})); window.dispatchEvent(new PointerEvent('pointerup'))`)
    await key(Key.ARROW_UP)
    await driver.wait(async () => Number((await scores())[0]) > 0, 3000)
    await driver.executeScript('window.dispatchEvent(new Event("blur"))')
    await waitState('paused')
    const before = await scores()
    await driver.sleep(30000)
    assert.deepEqual(await scores(), before)
    await action()
    await waitState('active')
    await driver.wait(async () => Number((await scores())[0]) > Number(before[0]), 3000)
  })

  it('freezes on contact and starts ten clean replay attempts', async function () {
    await open()
    await action()
    for (let i = 0; i < 10; i++) {
      await waitState('finished')
      const final = await scores()
      assert.ok(Number(final[0]) > 0)
      assert.equal(final[1], '0')
      await key(Key.SPACE)
      assert.deepEqual(await scores(), final)
      await action()
      await waitState('active')
      assert.equal((await scores())[1], '0')
      assert.ok(Number((await scores())[0]) < 100)
    }
  })

  it('exits from ready, active, paused and finished and reopens cleanly', async function () {
    for (const screen of ['ready', 'active', 'paused', 'finished']) {
      for (const binding of ['q', 'link']) {
        await open()
        if (screen !== 'ready') await action()
        if (screen === 'paused') await driver.executeScript('window.dispatchEvent(new Event("blur"))')
        await waitState(screen)
        if (binding === 'q') await key('q')
        else await driver.findElement(By.id('exit')).click()
        await driver.wait(until.urlContains('#/'), 3000)
        assert.equal(new URL(await driver.getCurrentUrl()).pathname, new URL(root).pathname)
        assert.ok(await new OverviewPage(browser).isLoaded())
      }
    }
    await open()
    assert.deepEqual(await scores(), ['0', '0'])
  })

  it('exits during slow engine loading without attaching a late canvas', async function () {
    await driver.sendDevToolsCommand('Network.enable', {})
    await driver.sendDevToolsCommand('Network.emulateNetworkConditions', {
      offline: false, latency: 1800, downloadThroughput: 1000000, uploadThroughput: 1000000
    })
    try {
      await driver.get(new URL('bunny-hop', root).href)
      assert.equal(await state(), 'loading')
      await key('q')
      await driver.wait(until.urlContains('#/'), 10000)
      await driver.sleep(4000)
      assert.equal(await driver.findElements(By.css('#course canvas')).then(elements => elements.length), 0)
    } finally {
      await driver.sendDevToolsCommand('Network.emulateNetworkConditions', {
        offline: false, latency: 0, downloadThroughput: -1, uploadThroughput: -1
      })
    }
  })

  for (const blocked of ['*pixi-8.22.0.min.mjs*', '*unsafe-eval-8.22.0.min.js*', '*img/bunny-hop/rabbitmq-logo.png*']) {
    it(`offers exit when loading fails for ${blocked}`, async function () {
      await driver.sendDevToolsCommand('Network.enable', {})
      await driver.sendDevToolsCommand('Network.setBlockedURLs', { urls: [blocked] })
      try {
        for (const binding of ['q', 'link']) {
          await driver.get(new URL('bunny-hop', root).href)
          await waitState('error')
          assert.ok((await driver.findElement(By.id('status')).getText()).length > 0)
          if (binding === 'q') await key('q')
          else await driver.findElement(By.id('exit')).click()
          await driver.wait(until.urlContains('#/'), 3000)
        }
      } finally {
        await driver.sendDevToolsCommand('Network.setBlockedURLs', { urls: [] })
      }
    })
  }

  for (const preference of ['5000', '']) {
    for (const screen of ['ready', 'active', 'paused', 'finished']) {
      it(`keeps ${screen} isolated from refresh and preserves preference ${preference || 'disabled'}`, async function () {
        await driver.get(root)
        await driver.executeScript('store_pref("interval", arguments[0])', preference)
        await open()
        if (screen !== 'ready') await action()
        if (screen === 'paused') await driver.executeScript('window.dispatchEvent(new Event("blur"))')
        await waitState(screen)
        if (screen === 'active') {
          await driver.executeAsyncScript(function (done) {
            import(new URL('js/bunny-hop/model.js', location.href)).then(model => {
              let seed = 17, threshold = 930, spawn = 180
              const jump = () => {
                if (document.body.dataset.state !== 'active') return
                if (Number(document.getElementById('distance').textContent) >= threshold) {
                  document.getElementById('course').dispatchEvent(new PointerEvent('pointerdown', {isPrimary:true, button:0}))
                  window.dispatchEvent(new PointerEvent('pointerup'))
                  seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0
                  spawn += model.MIN_GAP + seed % 100
                  threshold = 750 + Math.ceil(spawn / (model.SPEED * model.STEP)) * model.SPEED * model.STEP
                }
                requestAnimationFrame(jump)
              }
              requestAnimationFrame(jump)
              done()
            }, error => done(String(error)))
          })
        }
        await driver.executeScript('window.bunnyCanvas = document.querySelector("canvas"); window.bunnyStart = performance.timeOrigin')
        const before = await scores()
        await driver.sleep(12000)
        assert.ok(await driver.executeScript('return window.bunnyCanvas === document.querySelector("canvas") && window.bunnyStart === performance.timeOrigin'))
        assert.ok(await driver.executeScript(`return !performance.getEntriesByType('resource').some(e => new URL(e.name).pathname.includes('/api/'))`))
        assert.equal(await driver.executeScript('return localStorage.getItem("rabbitmq.interval")'), preference)
        assert.equal(await state(), screen)
        if (screen !== 'active') assert.deepEqual(await scores(), before)
        await key('q')
        await driver.wait(until.urlContains('#/'), 3000)
        assert.ok(await new OverviewPage(browser).isLoaded())
        assert.equal(await driver.executeScript('return get_pref("interval")'), preference)
        await driver.executeScript('performance.clearResourceTimings()')
        await driver.sleep(6000)
        const polled = await driver.executeScript(`return performance.getEntriesByType('resource').some(e => new URL(e.name).pathname.includes('/api/overview'))`)
        assert.equal(polled, preference !== '')
      })
    }
  }
})
