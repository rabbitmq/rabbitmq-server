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
const TRACING_JS = path.join(__dirname, '../priv/www/js/tracing.js');

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
