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
const ROUTER_JS =
    path.join(__dirname, '../../rabbitmq_management/priv/www/js/router.js');
const TRACING_JS = path.join(__dirname, '../priv/www/js/tracing.js');

function loadPlugin(pluginPath, nodesJson) {
    var calls = [];
    var docHandlers = {};
    var documentStub = { title: '' };
    var dispatchers = [];

    var jqStub = function(target) {
        if (target === documentStub) {
            return {
                on: function(event, selector, fn) { docHandlers[event + ' ' + selector] = fn; },
                off: function() {}
            };
        }
        return {
            serializeArray: function() { return (target && target._fields) || []; },
            serialize: function() { return ''; },
            val: function() { return ''; }
        };
    };

    var sandbox = {
        window: {
            location: { hash: '' },
            addEventListener: function() {},
            removeEventListener: function() {}
        },
        document: documentStub,
        location: { href: '' },
        $: jqStub,
        jQuery: jqStub,
        console: console,
        JSON: JSON,
        Object: Object,
        parseInt: parseInt,
        NAVIGATION: { 'Admin': [{}] },
        HELP: {},
        COLUMNS: {},
        disable_stats: false,
        QUEUE_EXTRA_CONTENT_REQUESTS: [],
        QUEUE_EXTRA_CONTENT: [],
        RENDER_CALLBACKS: {},
        CONSUMER_OWNER_FORMATTERS: [],
        CONSUMER_OWNER_FORMATTERS_COMPARATOR: function() { return 0; },
        dispatcher_add: function(fn) { dispatchers.push(fn); },
        esc: encodeURIComponent,
        render: function(reqs, template, redirect) {
            calls.push({ fn: 'render', reqs: reqs, template: template, redirect: redirect });
        },
        go_to: function(url) { calls.push({ fn: 'go_to', url: url }); },
        sync_get: function() {
            return nodesJson === undefined
                ? JSON.stringify([{ name: 'rabbit@host1' }])
                : nodesJson;
        },
        put_cast_params: function(sammy, p, mandatory, numeric, boolean) {
            calls.push({
                fn: 'put_cast_params',
                path: p,
                params: Object.assign({}, sammy.params),
                mandatory: Array.from(mandatory),
                numeric: Array.from(numeric),
                boolean: Array.from(boolean)
            });
        },
        _docHandlers: docHandlers
    };
    vm.createContext(sandbox);
    vm.runInContext(fs.readFileSync(ROUTER_JS, 'utf8'), sandbox, { filename: ROUTER_JS });
    vm.runInContext(fs.readFileSync(pluginPath, 'utf8'), sandbox, { filename: pluginPath });

    var app = vm.runInContext('(function(dispatchers) {' +
        'return new Application(function() {' +
        'var self = this; dispatchers.forEach(function(d) { d(self); }); });' +
        '})', sandbox)(dispatchers);

    return { app: app, calls: calls, sandbox: sandbox };
}

describe('tracing routes', () => {
    it('redirects #/traces to the first node', () => {
        var plugin = loadPlugin(TRACING_JS);
        plugin.app._runRoute('get', '#/traces');
        assert.deepEqual(Array.from(plugin.calls),
                         [{ fn: 'go_to', url: '#/traces/rabbit@host1' }]);
    });

    it('does not navigate when the node list cannot be fetched', () => {
        var plugin = loadPlugin(TRACING_JS, false);
        plugin.app._runRoute('get', '#/traces');
        assert.deepEqual(Array.from(plugin.calls), []);
    });

    it('does not navigate when the node list is an empty body', () => {
        var plugin = loadPlugin(TRACING_JS, '');
        plugin.app._runRoute('get', '#/traces');
        assert.deepEqual(Array.from(plugin.calls), []);
    });

    it('does not navigate when no node is listed', () => {
        var plugin = loadPlugin(TRACING_JS, '[]');
        plugin.app._runRoute('get', '#/traces');
        assert.deepEqual(Array.from(plugin.calls), []);
    });

    it('renders the traces of one node', () => {
        var plugin = loadPlugin(TRACING_JS);
        plugin.app._runRoute('get', '#/traces/rabbit%40host1');
        var call = plugin.calls[0];
        assert.equal(call.template, 'traces');
        assert.equal(call.reqs.traces, '/traces/node/rabbit%40host1');
        assert.equal(call.redirect, '#/traces');
    });
});
