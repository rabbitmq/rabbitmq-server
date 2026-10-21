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
const JS_DIR = path.join(__dirname, '../priv/www/js');

function load(sandbox, file) {
    const filename = path.join(JS_DIR, file);
    vm.runInContext(fs.readFileSync(filename, 'utf8'), sandbox, { filename });
}

const replaced = {};
const consoleErrors = [];
const jqStub = function(selector) {
    return {
        ready: function() {},
        html: function(html) { replaced[selector] = html; }
    };
};
jqStub.fn = { extend: function() {} };

const sandbox = {
    $: jqStub,
    jQuery: jqStub,
    window: {},
    document: {},
    console: { error: function() { consoleErrors.push(arguments); } },
    COMPILED_TEMPLATES: { status: function(model) { return model.text; } },
    timer_interval: 5000,
    QUEUE_TYPE: { quorum: { params: {} } }
};
vm.createContext(sandbox);
load(sandbox, 'formatters.js');
load(sandbox, 'main.js');

// Behaves like `XMLHttpRequest`, which reports `abort()` through
// `onreadystatechange` with a status of 0.
function fakeRequest(onDone) {
    return {
        readyState: 1,
        status: 0,
        onreadystatechange: onDone,
        abort: function() {
            this.readyState = 4;
            if (this.onreadystatechange) this.onreadystatechange();
        }
    };
}

describe('abort_outstanding_reqs', () => {
    it('aborts every outstanding request without running its handler', () => {
        var handled = 0;
        var reqs = [fakeRequest(() => handled++), fakeRequest(() => handled++)];
        sandbox.outstanding_reqs = reqs.slice();
        sandbox.abort_outstanding_reqs();
        assert.equal(handled, 0);
        assert.deepEqual(reqs.map(r => r.readyState), [4, 4]);
        assert.equal(sandbox.outstanding_reqs.length, 0);
    });
});

describe('update_status', () => {
    it('reports an error before any request has succeeded', () => {
        sandbox.last_successful_connect = undefined;
        sandbox.update_status('error');
        assert.match(replaced['#status'], /^Error: could not connect to server\. Will retry at /);
    });

    it('reports when the last request succeeded', () => {
        sandbox.last_successful_connect = new Date(2026, 0, 2, 3, 4, 5);
        sandbox.update_status('error');
        assert.match(replaced['#status'], /^Error: could not connect to server since 2026-01-02 03:04:05\. /);
    });

    // `check_bad_response` passes this for HTTP 408.
    it('reports a request that timed out', () => {
        sandbox.update_status('timeout');
        assert.match(replaced['#status'],
                     /^Error: timed out talking to the server\. Will retry at /);
    });

    it('still rejects a status it has no text for', () => {
        assert.throws(function() { sandbox.update_status('bogus'); },
                      /Unknown status bogus/);
    });
});

describe('dispatcher', () => {
    it('adds the routes of other extensions when one of them throws', () => {
        var routes = [];
        var app = {
            setTitle: function() {},
            get: function(path) { routes.push(path); }
        };
        sandbox.dispatcher_modules = [
            function(sammy) { sammy.get('#/'); },
            function(sammy) { sammy.redirect('#/'); },
            function(sammy) { sammy.get('#/plugin'); }
        ];
        sandbox.dispatcher.call(app);
        assert.deepEqual(routes, ['#/', '#/plugin']);
        assert.equal(consoleErrors.length, 1);
    });
});

describe('start_app_login', () => {
    function startAppLogin(oauth, hasCredentials, loginSucceeds) {
        var runs = 0;
        sandbox.Application = function() {};
        sandbox.Application.prototype.run = function() { runs++; };
        sandbox.oauth = oauth;
        sandbox.has_auth_credentials = function() { return hasCredentials; };
        sandbox.check_login = function() { return loginSucceeds; };
        sandbox.start_app_login();
        return runs;
    }

    [{ enabled: false }, { enabled: true }].forEach((oauth) => {
        it(`handles the login form when stored credentials are rejected, OAuth enabled: ${oauth.enabled}`, () => {
            assert.equal(startAppLogin(oauth, true, false), 1);
        });

        it(`handles the login form without stored credentials, OAuth enabled: ${oauth.enabled}`, () => {
            assert.equal(startAppLogin(oauth, false, false), 1);
        });

        it(`leaves the routes to the UI after a login, OAuth enabled: ${oauth.enabled}`, () => {
            assert.equal(startAppLogin(oauth, true, true), 0);
        });
    });
});

describe('put_cast_params', () => {
    it('accepts a missing list of keys', () => {
        var put = null;
        sandbox.sync_put = function(sammy, path) { put = sammy.params; return false; };
        sandbox.put_cast_params({ params: { name: 'x', empty: '', count: '3' } },
                                '/x', undefined, ['count'], undefined);
        assert.deepEqual(Object.assign({}, put), { name: 'x', count: 3 });
    });

    // `router.test.js` asserts a parameter can be named `hasOwnProperty`.
    it('casts parameters that shadow a method on Object.prototype', () => {
        var put = null;
        sandbox.sync_put = function(sammy, path) { put = sammy.params; return false; };
        sandbox.put_cast_params({ params: { hasOwnProperty: 'x', count: '3' } },
                                '/x', undefined, ['count'], undefined);
        assert.deepEqual(Object.assign({}, put),
                         { hasOwnProperty: 'x', count: 3 });
    });
});

describe('collapse_multifields', () => {
    it('reads a queue type out of parameters that shadow a method', () => {
        const params = sandbox.collapse_multifields(
            { hasOwnProperty: 'x', queuetype: 'quorum' });
        assert.equal(params['arguments']['x-queue-type'], 'quorum');
        assert.equal(params.queuetype, undefined);
    });
});
