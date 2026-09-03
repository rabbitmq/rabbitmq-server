import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';

// Mock window & localStorage before testing functions that call prefs.js
const localStorageStore = new Map();
const mockLocalStorage = {
    getItem(key) {
        return localStorageStore.has(key) ? localStorageStore.get(key) : null;
    },
    setItem(key, value) {
        localStorageStore.set(key, String(value));
    },
    removeItem(key) {
        localStorageStore.delete(key);
    },
    clear() {
        localStorageStore.clear();
    }
};

globalThis.window = {
    localStorage: mockLocalStorage
};

import {
    prefix_title,
    chart_h3,
    node_stat_bar,
    node_stat_count,
    node_stat_count_bar,
    rates_text,
    update_rate_options
} from '../priv/www/js/charts.js';
import { ALL_CHART_RANGES } from '../priv/www/js/global.js';
import { store_pref, get_pref } from '../priv/www/js/prefs.js';

describe('charts: unit tests', () => {
    beforeEach(() => {
        localStorageStore.clear();
        ALL_CHART_RANGES['lengths-over'] = 'Last 10 minutes';
        ALL_CHART_RANGES['global'] = 'Global range';
    });

    it('prefix_title returns expected descriptions', () => {
        assert.equal(prefix_title('chart', 'global'), 'global range');
        assert.equal(prefix_title('curr', 'global'), 'current value');
        assert.equal(prefix_title('avg', 'global'), 'moving average: global range');
    });

    it('chart_h3 generates proper h3 tag', () => {
        const html = chart_h3('test-id', 'Test Heading', 'help-id');
        assert.ok(html.includes('<h3>Test Heading'));
        assert.ok(html.includes('class="popup-options-link"'));
        assert.ok(html.includes('id="help-id"'));
    });

    it('node_stat_bar constructs status bar HTML correctly', () => {
        const stats = { fd_used: 100, fd_total: 1000 };
        const fmt = (v) => `${v} units`;
        const html = node_stat_bar('fd_used', 'fd_total', 'available', stats, fmt, 'green', 'help-fd', false);

        assert.ok(html.includes('class="status-bar"'));
        assert.ok(html.includes('100 units'));
        assert.ok(html.includes('1000 units available'));
        assert.ok(html.includes('id="help-fd"'));
    });

    it('node_stat_count formats count when numeric or returns non-numeric value as is', () => {
        store_pref('rate-mode-node-stats', 'bar');

        const statsNum = { fd_used: 50, fd_total: 100 };
        const resNum = node_stat_count('fd_used', 'fd_total', statsNum, {});
        assert.ok(resNum.includes('status-bar'));

        const statsStr = { fd_used: 'N/A', fd_total: 100 };
        const resStr = node_stat_count('fd_used', 'fd_total', statsStr, {});
        assert.equal(resStr, 'N/A');
    });

    it('node_stat_count_bar formats bar when numeric or returns non-numeric value as is', () => {
        const statsNum = { proc_used: 20, proc_total: 100 };
        const resNum = node_stat_count_bar('proc_used', 'proc_total', statsNum, {});
        assert.ok(resNum.includes('status-bar'));

        const statsStr = { proc_used: 'disabled', proc_total: 100 };
        const resStr = node_stat_count_bar('proc_used', 'proc_total', statsStr, {});
        assert.equal(resStr, 'disabled');
    });

    it('rates_text formats stats entries into HTML box', () => {
        const items = [['Publish', 'publish']];
        const stats = { publish_details: { rate: 5.5 } };
        const fmt = (v) => `${v}/s`;

        const html = rates_text(items, stats, 'avg', fmt, true);
        assert.ok(html.includes('class="box"'));
        assert.ok(html.includes('Publish'));
    });

    it('update_rate_options updates preferences via sammy parameters', () => {
        const sammy = {
            params: {
                id: 'msg-rates',
                mode: 'chart',
                size: 'medium',
                range: 'global'
            }
        };

        update_rate_options(sammy);

        assert.equal(get_pref('rate-mode-msg-rates'), 'chart');
        assert.equal(get_pref('chart-size-msg-rates'), 'medium');
        assert.equal(get_pref('chart-range'), 'global');
    });
});
