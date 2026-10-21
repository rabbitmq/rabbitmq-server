// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadPlugin } from '../../rabbitmq_management/test/plugin-sandbox.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const TOP_JS = path.join(__dirname, '../priv/www/js/top.js');

describe('top routes', () => {
    it('redirects #/top to the first node with 20 rows', () => {
        var plugin = loadPlugin(TOP_JS);
        plugin.app._runRoute('get', '#/top');
        assert.deepEqual(plugin.calls,
                         [{ fn: 'go_to_first_node', prefix: '#/top/', suffix: '/20' }]);
    });

    it('redirects #/top/ets to the first node with 20 rows', () => {
        var plugin = loadPlugin(TOP_JS);
        plugin.app._runRoute('get', '#/top/ets');
        assert.deepEqual(plugin.calls,
                         [{ fn: 'go_to_first_node', prefix: '#/top/ets/', suffix: '/20' }]);
    });

    it('renders processes for a node and row count', () => {
        var plugin = loadPlugin(TOP_JS);
        plugin.app._runRoute('get', '#/top/rabbit%40host1/50');
        var call = plugin.calls[0];
        assert.equal(call.template, 'processes');
        assert.equal(call.reqs.top.path, '/top/rabbit%40host1');
        assert.equal(call.reqs.top.options.row_count, '50');
        assert.equal(call.redirect, '#/top');
    });

    it('renders ETS tables for a node and row count', () => {
        var plugin = loadPlugin(TOP_JS);
        plugin.app._runRoute('get', '#/top/ets/rabbit%40host1/100');
        var call = plugin.calls[0];
        assert.equal(call.template, 'ets_tables');
        assert.equal(call.reqs.top.path, '/top/ets/rabbit%40host1');
        assert.equal(call.reqs.top.options.row_count, '100');
        assert.equal(call.redirect, '#/top/ets');
    });

    it('does not mistake an ETS path for a process path', () => {
        var plugin = loadPlugin(TOP_JS);
        plugin.app._runRoute('get', '#/top/ets/rabbit%40host1/20');
        assert.equal(plugin.calls.length, 1);
        assert.equal(plugin.calls[0].template, 'ets_tables');
    });

    it('decodes a process id and encodes it once for the API path', () => {
        var plugin = loadPlugin(TOP_JS);
        plugin.app._runRoute('get', '#/process/%3C0.123.0%3E');
        var call = plugin.calls[0];
        assert.equal(call.template, 'process');
        assert.equal(call.reqs.process, '/process/%3C0.123.0%3E');
    });

    it('ignores a query string when matching', () => {
        var plugin = loadPlugin(TOP_JS);
        plugin.app._runRoute('get', '#/top/rabbit%40host1/20?x=1');
        assert.equal(plugin.calls[0].reqs.top.options.row_count, '20');
    });

    it('registers its change handlers on the document', () => {
        var plugin = loadPlugin(TOP_JS);
        var keys = Object.keys(plugin.sandbox._docHandlers);
        assert.ok(keys.includes('change select#top-node'));
        assert.ok(keys.includes('change select#row-count-ets'));
    });
});
