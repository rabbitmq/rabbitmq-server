// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CHARTS_JS_PATH = path.join(__dirname, '../priv/www/js/charts.js');
const chartsSrc = fs.readFileSync(CHARTS_JS_PATH, 'utf8');

// charts.js only declares functions and vars at load time, so it runs with
// stubs for the browser and jQuery globals its functions reference.
const sandbox = {
  $: () => ({}),
  jQuery: () => ({}),
  get_pref: () => undefined,
  store_pref: () => undefined,
};
vm.createContext(sandbox);
vm.runInContext(chartsSrc, sandbox, { filename: CHARTS_JS_PATH });

describe('chart_chrome', () => {
  it('sets timeBase to milliseconds to match RabbitMQ millisecond-epoch timestamps', () => {
    assert.equal(sandbox.chart_chrome.xaxis.timeBase, 'milliseconds');
  });

  it('keeps the time mode and browser timezone', () => {
    assert.equal(sandbox.chart_chrome.xaxis.mode, 'time');
    assert.equal(sandbox.chart_chrome.xaxis.timezone, 'browser');
  });
});

describe('chart y-axis range', () => {
  it('starts at zero for data far from zero', () => {
    const range = { xmin: 1, xmax: 2, ymin: 1000, ymax: 1000 };
    for (const hook of sandbox.chart_chrome.hooks.adjustSeriesDataRange) {
      hook(null, {}, range);
    }
    assert.equal(range.ymin, 0);
    assert.equal(range.ymax, 1000);
  });
});
