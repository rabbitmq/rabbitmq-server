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
const STREAM_JS = path.join(__dirname, '../priv/www/js/stream.js');

function loadPlugin(pluginPath) {
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
        sync_get: function() { return JSON.stringify([{ name: 'rabbit@host1' }]); },
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

function submitForm(plugin, method, action, fields) {
    var form = {
        getAttribute: function(name) {
            if (name === 'method') return method;
            if (name === 'action') return action;
            return null;
        },
        _fields: fields
    };
    plugin.sandbox._docHandlers['submit form'](
        { target: form, currentTarget: form, preventDefault: function() {} });
}

describe('stream management routes', () => {
    it('registers its routes through dispatcher_add', () => {
        var plugin = loadPlugin(STREAM_JS);
        var routes = plugin.app._routes;
        assert.equal(routes.get.length, 3);
        assert.equal(routes.put.length, 1);
    });

    it('renders the connection list', () => {
        var plugin = loadPlugin(STREAM_JS);
        plugin.sandbox.url_pagination_template_context = function() { return 'pagination'; };
        plugin.app._runRoute('get', '#/stream/connections');
        assert.equal(plugin.calls.length, 1);
        assert.equal(plugin.calls[0].template, 'streamConnections');
        assert.equal(plugin.calls[0].redirect, '#/stream/connections');
    });

    it('decodes the vhost and name once before building API paths', () => {
        var plugin = loadPlugin(STREAM_JS);
        plugin.app._runRoute('get', '#/stream/connections/%2F/127.0.0.1%3A5552%20-%3E%20127.0.0.1%3A5552');
        var call = plugin.calls[0];
        var base = '/stream/connections/%2F/127.0.0.1%3A5552%20-%3E%20127.0.0.1%3A5552';
        assert.equal(call.template, 'streamConnection');
        assert.equal(call.reqs.connection.path, base);
        assert.equal(call.reqs.consumers, base + '/consumers');
        assert.equal(call.reqs.publishers, base + '/publishers');
    });

    it('renders the super streams page', () => {
        var plugin = loadPlugin(STREAM_JS);
        plugin.app._runRoute('get', '#/stream/super-streams');
        assert.equal(plugin.calls[0].template, 'superStreams');
        assert.equal(plugin.calls[0].reqs.vhosts, '/vhosts');
    });

    it('routes the super stream form to the put handler with its fields', () => {
        var plugin = loadPlugin(STREAM_JS);
        plugin.app.run();
        submitForm(plugin, 'put', '#/stream/super-streams', [
            { name: 'vhost', value: '/' },
            { name: 'name', value: 'invoices' },
            { name: 'partitions', value: '3' },
            { name: 'binding-keys', value: '' }
        ]);
        assert.equal(plugin.calls.length, 1);
        var call = plugin.calls[0];
        assert.equal(call.fn, 'put_cast_params');
        assert.equal(call.path, '/stream/super-streams/:vhost/:name');
        assert.equal(call.params.vhost, '/');
        assert.equal(call.params.name, 'invoices');
        assert.equal(call.params.partitions, '3');
        assert.equal(plugin.sandbox.location.href, '/#/queues');
        plugin.app.unload();
    });

    it('does not run the put handler for a get of the same path', () => {
        var plugin = loadPlugin(STREAM_JS);
        plugin.app._runRoute('get', '#/stream/super-streams');
        assert.equal(plugin.calls.filter(c => c.fn === 'put_cast_params').length, 0);
    });

    it('adds a queue page extra content request using the decoded names', () => {
        var plugin = loadPlugin(STREAM_JS);
        var extra = plugin.sandbox.QUEUE_EXTRA_CONTENT_REQUESTS[0]('/', 'my queue');
        assert.equal(extra.extra_stream_publishers, '/stream/publishers/%2F/my%20queue');
    });
});
