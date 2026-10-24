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
const SHOVEL_JS_PATH = path.join(__dirname, '../priv/www/js/shovel.js');
const shovelSrc = fs.readFileSync(SHOVEL_JS_PATH, 'utf8');

// shovel.js registers its routes at load time via a single top-level
// dispatcher_add(function(sammy) { ... }) call, plus a couple of other
// top-level statements (NAVIGATION/HELP entries) that need a minimal shape
// to not throw. None of that touches the DOM, so three small stubs are
// enough to load the file.
function loadShovelModule() {
  let dispatcherCallback;
  const sandbox = {
    dispatcher_add: (fn) => { dispatcherCallback = fn; },
    NAVIGATION: { Admin: [{}] },
    HELP: {}
  };
  vm.createContext(sandbox);
  vm.runInContext(shovelSrc, sandbox, { filename: SHOVEL_JS_PATH });
  return { sandbox, dispatcherCallback };
}

// A stand-in for Sammy's routing DSL: records what got registered instead
// of actually routing anything, so registration can be asserted on without
// pulling in Sammy or exercising the (heavier, DOM/network-touching)
// handler bodies themselves.
function fakeSammy() {
  const routes = [];
  const record = (method) => (path, handler) => routes.push({ method, path, handler });
  return {
    routes,
    get: record('GET'),
    put: record('PUT'),
    del: record('DELETE')
  };
}

describe('shovel.js route registration', () => {
  it('registers its routes through dispatcher_add', () => {
    const { dispatcherCallback } = loadShovelModule();
    assert.equal(typeof dispatcherCallback, 'function');
  });

  it('registers exactly the expected methods and paths', () => {
    const { dispatcherCallback } = loadShovelModule();
    const sammy = fakeSammy();

    dispatcherCallback(sammy);

    assert.deepEqual(
      sammy.routes.map(({ method, path }) => [method, path]),
      [
        ['GET', '#/shovels'],
        ['GET', '#/dynamic-shovels'],
        ['GET', '#/dynamic-shovels/:vhost/:id'],
        ['PUT', '#/shovel-parameters-move-messages'],
        ['PUT', '#/shovel-parameters'],
        ['DELETE', '#/shovel-parameters'],
        ['DELETE', '#/shovel-restart-link']
      ]
    );
  });

  it('registers a handler function for every route', () => {
    const { dispatcherCallback } = loadShovelModule();
    const sammy = fakeSammy();

    dispatcherCallback(sammy);

    assert.ok(sammy.routes.length > 0);
    for (const { method, path, handler } of sammy.routes) {
      assert.equal(typeof handler, 'function', `${method} ${path} should register a function`);
    }
  });
});


describe('Move messages credentials', () => {
  function submit(administrator, password) {
    const { sandbox, dispatcherCallback } = loadShovelModule();
    const sammy = fakeSammy();
    dispatcherCallback(sammy);
    Object.assign(sandbox, {
      user_administrator: administrator,
      user: { name: 'policy@user' },
      esc: encodeURIComponent,
      put_parameter: (request) => { sandbox.submitted = { ...request.params }; }
    });
    const params = {
      vhost: 'tenant/vhost',
      'src-uri': 'amqp:///tenant%2Fvhost',
      'dest-uri': 'amqp:///tenant%2Fvhost'
    };
    if (password !== undefined) params['move-password'] = password;
    const route = sammy.routes.find(r => r.path === '#/shovel-parameters-move-messages');
    route.handler.call({ params });
    return sandbox.submitted;
  }

  it('encodes explicit credentials for both connections and removes the form field', () => {
    const result = submit(false, 'secret:@/%?&');
    const uri = 'amqp://policy%40user:secret%3A%40%2F%25%3F%26@/tenant%2Fvhost';
    assert.equal(result['src-uri'], uri);
    assert.equal(result['dest-uri'], uri);
    assert.equal('move-password' in result, false);
  });

  it('accepts a token as the password', () => {
    const result = submit(false, 'header.payload.signature');
    assert.equal(result['src-uri'],
      'amqp://policy%40user:header.payload.signature@/tenant%2Fvhost');
  });

  it('requires a nonempty credential from a non-administrator', () => {
    for (const password of [undefined, '']) {
      assert.throws(() => submit(false, password), /password or token/);
    }
  });

  it('keeps administrator connections passwordless', () => {
    const result = submit(true);
    assert.equal(result['src-uri'], 'amqp:///tenant%2Fvhost');
    assert.equal(result['dest-uri'], 'amqp:///tenant%2Fvhost');
  });
});
