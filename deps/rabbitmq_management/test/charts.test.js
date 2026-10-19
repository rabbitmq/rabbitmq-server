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

  it('draws lines as thick as Flot 0.8 did by default', () => {
    assert.equal(sandbox.chart_chrome.series.lines.lineWidth, 2);
  });

  it('leaves the grid line colour to the grid options', () => {
    assert.equal(sandbox.chart_chrome.xaxis.tickColor, undefined);
    assert.equal(sandbox.chart_chrome.yaxis.tickColor, undefined);
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

  it('treats an empty series as an idle one', () => {
    const range = { xmin: Infinity, xmax: -Infinity, ymin: Infinity, ymax: -Infinity };
    for (const hook of sandbox.chart_chrome.hooks.adjustSeriesDataRange) {
      hook(null, {}, range);
    }
    assert.equal(range.ymin, 0);
    assert.equal(range.ymax, 0);
  });
});

describe('chart grid', () => {
  it('hides the vertical grid lines Flot 4 draws by default', () => {
    assert.equal(sandbox.chart_chrome.xaxis.gridLines, false);
  });

  it('measures every tick label, so a two-tick axis reserves room', () => {
    assert.equal(sandbox.chart_chrome.yaxis.showTickLabels, 'all');
  });
});

// These drive `render_chart`, so they need stubs for jQuery, the DOM and
// the globals declared in `global.js`.
function render(opts) {
  const plotted = [];
  let computed_style_reads = 0;
  const box = {
    console,
    chart_data: {},
    document: { documentElement: {} },
    getComputedStyle: () => {
      computed_style_reads++;
      return { getPropertyValue: () => opts.grid_css };
    },
    get_pref: () => undefined,
    store_pref: () => undefined,
  };
  const ids = opts.ids === undefined ? ['a'] : opts.ids;
  const divs = ids.map((id) => ({
    attr: () => 'chart-' + id,
    hasClass: (name) => name === 'chart-full',
  }));
  box.$ = function(selector) {
    if (selector === '.chart') {
      return { length: divs.length,
               map: (fn) => divs.map((div) => fn.call(div)) };
    }
    return selector;
  };
  // `render_chart` hands the same module-level `chart_chrome` to every
  // chart, so the values have to be read here rather than kept by reference.
  box.$.plot = (div, data, options) => {
    plotted.push({
      series: data.length,
      yaxis_max: options.yaxis.max,
      tick_color: options.grid.tickColor,
      tick_formatter: options.yaxis.tickFormatter,
    });
  };
  vm.createContext(box);
  vm.runInContext(chartsSrc, box, { filename: CHARTS_JS_PATH });
  for (const id of ids) {
    const data = opts.data_by_id ? opts.data_by_id[id] : opts.data;
    box.chart_data[id] = { data: data,
                           fmt: opts.fmt || ((v) => String(v)) };
  }
  box.render_charts();
  return { plotted: plotted, computed_style_reads: computed_style_reads };
}

const ONE_SERIES = {
  publish: { ix: 0, samples: [{ timestamp: 2000, sample: 8 },
                              { timestamp: 1000, sample: 4 }] },
};

describe('rendering a chart', () => {
  it('gives an axis with no series the same range as an idle one', () => {
    const { plotted } = render({ data: {}, grid_css: '#ddd' });
    assert.equal(plotted[0].series, 0);
    assert.equal(plotted[0].yaxis_max, 1);
  });

  it('leaves the axis range to the data when there are series', () => {
    const { plotted } = render({ data: ONE_SERIES, grid_css: '#ddd' });
    assert.equal(plotted[0].series, 1);
    assert.equal(plotted[0].yaxis_max, null);
  });

  it('gives each chart on the page its own axis range', () => {
    const { plotted } = render({
      ids: ['empty', 'full'],
      data_by_id: { empty: {}, full: ONE_SERIES },
      grid_css: '#ddd',
    });
    assert.deepEqual(plotted.map((p) => [p.series, p.yaxis_max]),
                     [[0, 1], [1, null]]);
  });

  it('scales a tick label against the whole axis, not the value', () => {
    const { plotted } = render({ data: ONE_SERIES, grid_css: '#ddd',
                                 fmt: (v, max) => v + ' of ' + max });
    assert.equal(plotted[0].tick_formatter(2, { max: 8 }), '2 of 8');
  });

  it('reads no style at all on a page with no charts', () => {
    const { plotted, computed_style_reads } =
      render({ ids: [], data: ONE_SERIES, grid_css: '#ddd' });
    assert.equal(computed_style_reads, 0);
    assert.equal(plotted.length, 0);
  });

  it('reads the grid colour once however many charts are on the page', () => {
    const { plotted, computed_style_reads } =
      render({ ids: ['a', 'b', 'c'], data: ONE_SERIES, grid_css: '#123456' });
    assert.equal(computed_style_reads, 1);
    assert.deepEqual(plotted.map((p) => p.tick_color),
                     ['#123456', '#123456', '#123456']);
  });

  it('falls back to a grid colour when the custom property is unset', () => {
    const { plotted } = render({ data: ONE_SERIES, grid_css: '' });
    assert.equal(plotted[0].tick_color, '#aaa');
  });
});

describe('chart y-axis ticks', () => {
  it('leaves an idle chart axis to Flot, which labels its two ends', () => {
    const axis = { max: 1, tickGenerator: () => [0, 0.5, 1] };
    assert.equal(Array.from(sandbox.chart_chrome.yaxis.ticks(axis)).length, 0);
  });

  it('uses the generated ticks otherwise', () => {
    const axis = { max: 1500, tickGenerator: () => [0, 500, 1000, 1500] };
    assert.deepEqual(sandbox.chart_chrome.yaxis.ticks(axis), [0, 500, 1000, 1500]);
  });
});
