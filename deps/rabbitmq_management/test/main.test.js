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
    timer_interval: 5000
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
