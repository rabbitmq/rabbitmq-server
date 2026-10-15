// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PREFS_JS_PATH = path.join(__dirname, '../priv/www/js/prefs.js');
const prefsSrc = fs.readFileSync(PREFS_JS_PATH, 'utf8');

let storage;
let sandbox;

function loadPrefsModule() {
  storage = {};
  sandbox = {
    console: { error: () => {} },
    $: { ajax: () => {} },
    window: {
      localStorage: {
        setItem: (k, v) => { storage[k] = String(v); },
        getItem: (k) => (k in storage ? storage[k] : null),
        removeItem: (k) => { delete storage[k]; }
      }
    }
  };
  vm.createContext(sandbox);
  vm.runInContext(prefsSrc, sandbox, { filename: PREFS_JS_PATH });
}

describe('prefs.js set_auth listeners', () => {
  beforeEach(loadPrefsModule);

  it('notifies a registered listener after the credentials are stored', () => {
    let seen;
    sandbox.register_set_auth_listener('test', () => {
      seen = sandbox.get_auth_credentials();
    });

    sandbox.set_auth('Bearer', 'token-1');

    assert.equal(seen, 'token-1');
  });

  it('notifies every registered listener', () => {
    const calls = [];
    sandbox.register_set_auth_listener('a', () => calls.push('a'));
    sandbox.register_set_auth_listener('b', () => calls.push('b'));

    sandbox.set_auth('Basic', 'x');

    assert.deepEqual(calls, ['a', 'b']);
  });

  it('replaces a listener registered under the same name', () => {
    const calls = [];
    sandbox.register_set_auth_listener('a', () => calls.push('old'));
    sandbox.register_set_auth_listener('a', () => calls.push('new'));

    sandbox.set_auth('Basic', 'x');

    assert.deepEqual(calls, ['new']);
  });

  it('stops notifying a listener that was unregistered', () => {
    const calls = [];
    sandbox.register_set_auth_listener('a', () => calls.push('a'));
    sandbox.unregister_set_auth_listener('a');

    sandbox.set_auth('Basic', 'x');

    assert.deepEqual(calls, []);
  });

  it('keeps notifying the other listeners when one throws', () => {
    const calls = [];
    sandbox.register_set_auth_listener('bad', () => { throw new Error('boom'); });
    sandbox.register_set_auth_listener('good', () => calls.push('good'));

    sandbox.set_auth('Basic', 'x');

    assert.deepEqual(calls, ['good']);
    assert.equal(sandbox.get_auth_credentials(), 'x');
  });

  it('does nothing when no listener is registered', () => {
    sandbox.set_auth('Basic', 'x');

    assert.equal(sandbox.get_auth_credentials(), 'x');
  });
});

describe('prefs.js login status message', () => {
  beforeEach(loadPrefsModule);

  it('returns the stored message', () => {
    sandbox.store_login_status_message('Session terminated or expired');

    assert.equal(sandbox.take_login_status_message(), 'Session terminated or expired');
  });

  it('returns a message only once', () => {
    sandbox.store_login_status_message('first');

    sandbox.take_login_status_message();

    assert.equal(sandbox.take_login_status_message(), null);
  });

  it('keeps only the last stored message', () => {
    sandbox.store_login_status_message('first');
    sandbox.store_login_status_message('second');

    assert.equal(sandbox.take_login_status_message(), 'second');
  });

  it('returns nothing when no message was stored', () => {
    assert.equal(sandbox.take_login_status_message(), null);
  });
});
