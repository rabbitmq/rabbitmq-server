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
const STREAM_JS = path.join(__dirname, '../priv/www/js/stream.js');

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
