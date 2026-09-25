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
const ROUTER_JS_PATH = path.join(__dirname, '../priv/www/js/router.js');
const routerSrc = fs.readFileSync(ROUTER_JS_PATH, 'utf8');

// router.js only declares functions at load time, so it loads once with
// minimal stubs and is shared across tests; each test isolates itself by
// creating its own Application instance.
function makeSandbox() {
    var listeners = {};
    var documentStub = { title: '' };
    var windowStub = {
        location: { hash: '', toString: function() { return 'http://localhost/' + this.hash; } },
        addEventListener: function(name, fn) { listeners[name] = fn; },
        removeEventListener: function(name, fn) { if (listeners[name] === fn) delete listeners[name]; }
    };
    // These tests call _runRoute directly rather than run(), so the stub
    // only needs to support serializeArray/serialize plus a no-op on/off
    // for the document-level submit delegation the router sets up.
    var jqStub = function() {
        return {
            serializeArray: function() { return []; },
            serialize: function() { return ''; },
            on: function() {},
            off: function() {}
        };
    };

    var sandbox = {
        window: windowStub,
        document: documentStub,
        $: jqStub,
        jQuery: jqStub,
        console: console
    };
    vm.createContext(sandbox);
    vm.runInContext(routerSrc, sandbox, { filename: ROUTER_JS_PATH });
    return sandbox;
}

const sandbox = makeSandbox();
const Application = sandbox.Application;

describe('route pattern compilation', () => {
    it('extracts named params from a multi-segment pattern', () => {
        var app = new Application();
        var seen = null;
        app.get('#/exchanges/:vhost/:name', function() { seen = this.params; });
        app._runRoute('get', '#/exchanges/myvhost/myexchange');
        assert.equal(seen.vhost, 'myvhost');
        assert.equal(seen.name, 'myexchange');
    });
});

describe('literal routes', () => {
    it('matches only the exact literal path', () => {
        var app = new Application();
        var calls = 0;
        app.get('#/', function() { calls++; });
        app._runRoute('get', '#/');
        assert.equal(calls, 1);
        app._runRoute('get', '#/other');
        assert.equal(calls, 1);
    });
});

describe('route order', () => {
    it('runs only the first registered match', () => {
        var app = new Application();
        var fired = [];
        app.get('#/queues', function() { fired.push('first'); });
        app.get('#/queues', function() { fired.push('second'); });
        app._runRoute('get', '#/queues');
        assert.deepEqual(fired, ['first']);
    });
});

describe('verb routing', () => {
    it('routes put requests with extra params merged in', () => {
        var app = new Application();
        var seen = null;
        app.put('#/login', function() { seen = this.params; });
        app._runRoute('put', '#/login', { username: 'guest' });
        assert.equal(seen.username, 'guest');
    });

    it('routes del requests', () => {
        var app = new Application();
        var calls = 0;
        app.del('#/queues', function() { calls++; });
        app._runRoute('del', '#/queues');
        assert.equal(calls, 1);
    });
});

describe('title', () => {
    it('applies the app title prefix set via setTitle', () => {
        var app = new Application();
        app.setTitle('RabbitMQ:');
        app.get('#/', function() { this.title('Overview'); });
        app._runRoute('get', '#/');
        assert.equal(sandbox.document.title, 'RabbitMQ: Overview');
    });
});

describe('params merging', () => {
    it('merges URL params and extra params into a single object', () => {
        var app = new Application();
        var seen = null;
        app.put('#/exchanges/:vhost/:name', function() { seen = this.params; });
        app._runRoute('put', '#/exchanges/myvhost/myexchange', { durable: 'true' });
        assert.equal(seen.vhost, 'myvhost');
        assert.equal(seen.name, 'myexchange');
        assert.equal(seen.durable, 'true');
    });
});

describe('no match', () => {
    it('does not throw when no route matches', () => {
        var app = new Application();
        assert.doesNotThrow(function() {
            app._runRoute('get', '#/does-not-exist');
        });
    });
});

describe('use', () => {
    it('is a no-op', () => {
        var app = new Application();
        assert.doesNotThrow(function() {
            app.use('Title');
        });
    });
});
