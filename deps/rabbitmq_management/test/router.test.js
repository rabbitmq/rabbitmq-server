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
    var docHandlers = {};
    var documentStub = { title: '' };
    var windowStub = {
        location: { hash: '', toString: function() { return 'http://localhost/' + this.hash; } },
        addEventListener: function(name, fn) { listeners[name] = fn; },
        removeEventListener: function(name, fn) { if (listeners[name] === fn) delete listeners[name]; }
    };
    // $(document) is used for submit delegation; every other call is
    // $(form), used to read the submitted fields.
    var jqStub = function(target) {
        if (target === documentStub) {
            return {
                on: function(event, selector, fn) { docHandlers[event] = fn; },
                off: function(event, selector, fn) { if (docHandlers[event] === fn) delete docHandlers[event]; }
            };
        }
        return {
            serializeArray: function() { return (target && target._fields) || []; },
            serialize: function() { return (target && target._qs) || ''; }
        };
    };

    var sandbox = {
        window: windowStub,
        document: documentStub,
        $: jqStub,
        jQuery: jqStub,
        console: console,
        _docHandlers: docHandlers,
        _listeners: listeners
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

describe('regular expression routes', () => {
    it('passes decoded unnamed groups as splat params and callback arguments', () => {
        var app = new Application();
        var seen = null;
        app.get(/^#\/plugin\/([^\/]+)\/(.+)$/, function(context, vhost, name) {
            seen = { context: context, vhost: vhost, name: name, splat: this.params.splat };
        });
        app._runRoute('get', '#/plugin/%2F/my%20queue?page=2');
        assert.equal(seen.vhost, '/');
        assert.equal(seen.name, 'my queue');
        assert.equal(Array.prototype.join.call(seen.splat, ','), '/,my queue');
        assert.equal(seen.context.params.page, '2');
    });
});

describe('route callbacks', () => {
    it('receive the route context as their first argument', () => {
        var app = new Application();
        var self = null;
        var arg = null;
        app.get('#/queues/:vhost', function(context) { self = this; arg = context; });
        app._runRoute('get', '#/queues/%2F');
        assert.equal(arg, self);
        assert.equal(arg.params.vhost, '/');
        assert.equal(arg.params.splat, undefined);
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

describe('form submission', () => {
    it('reads method via getAttribute and maps delete to the del route table', () => {
        var app = new Application();
        var seenParams = null;
        app.del('#/queues', function() { seenParams = this.params; });
        app.run();

        var prevented = false;
        // The DOM method property normalises to GET; getAttribute must be
        // used instead to observe the real value.
        var form = {
            method: 'GET',
            getAttribute: function(name) {
                if (name === 'method') return 'delete';
                if (name === 'action') return '#/queues';
                return null;
            },
            _fields: [{ name: 'name', value: 'q1' }, { name: 'tag', value: 'a' }, { name: 'tag', value: 'b' }]
        };
        sandbox._docHandlers.submit({ target: form, preventDefault: function() { prevented = true; } });

        assert.equal(prevented, true);
        assert.equal(seenParams.name, 'q1');
        // seenParams.tag is an array from the vm sandbox's own realm, so it
        // is compared by value rather than via assert.deepEqual, which
        // treats cross-realm arrays as unequal.
        assert.equal(Array.prototype.join.call(seenParams.tag, ','), 'a,b');

        app.unload();
    });
});

describe('route parameter decoding', () => {
    it('decodes percent-encoded named parameters', () => {
        var app = new Application();
        var seen;
        app.get('#/queues/:vhost/:name', function() { seen = this.params; });
        app._runRoute('get', '#/queues/%2F/my%20queue');
        assert.equal(seen.vhost, '/');
        assert.equal(seen.name, 'my queue');
    });

    it('does not include the query string in the last parameter', () => {
        var app = new Application();
        var seen;
        app.get('#/queues/:vhost/:name', function() { seen = this.params; });
        app._runRoute('get', '#/queues/%2F/cq?page=2');
        assert.equal(seen.name, 'cq');
    });

    it('merges decoded query parameters into params', () => {
        var app = new Application();
        var seen;
        app.get('#/queues', function() { seen = this.params; });
        app._runRoute('get', '#/queues?a=b%20c&tag=x&tag=y&plus=a+b');
        assert.equal(seen.a, 'b c');
        assert.deepEqual(Array.from(seen.tag), ['x', 'y']);
        assert.equal(seen.plus, 'a b');
    });
});

describe('location changes', () => {
    function navigate(hash) {
        sandbox.window.location.hash = hash;
        sandbox._listeners.hashchange();
    }

    it('does not run the start route again when the hash is set to it', () => {
        var app = new Application();
        var calls = 0;
        app.get('#/', function() { calls++; });
        sandbox.window.location.hash = '';
        app.run();
        navigate('#/');
        assert.equal(calls, 1);
        app.unload();
    });

    it('runs the matching route on every hash change', () => {
        var app = new Application();
        var seen = [];
        app.get('#/', function() { seen.push('overview'); });
        app.get('#/queues', function() { seen.push('queues'); });
        sandbox.window.location.hash = '#/';
        app.run();
        navigate('#/queues');
        navigate('#/');
        assert.deepEqual(seen, ['overview', 'queues', 'overview']);
        app.unload();
    });
});

describe('Sammy removal', () => {
    it('leaves no Sammy calls outside of the router', () => {
        const deps = path.join(__dirname, '../..');
        const offenders = fs.readdirSync(deps)
            .map(dep => path.join(deps, dep, 'priv/www/js'))
            .filter(dir => fs.existsSync(dir))
            .flatMap(dir => fs.readdirSync(dir, { recursive: true })
                .filter(file => file.endsWith('.js'))
                .map(file => path.join(dir, file)))
            .filter(file => file !== ROUTER_JS_PATH &&
                    /\bSammy\./.test(fs.readFileSync(file, 'utf8')));
        assert.deepEqual(offenders, []);
    });
});
