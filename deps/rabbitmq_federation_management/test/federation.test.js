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
const FEDERATION_JS_PATH = path.join(__dirname, '../priv/www/js/federation.js');
const federationSrc = fs.readFileSync(FEDERATION_JS_PATH, 'utf8');

// federation.js registers its routes at load time via a single top-level
// dispatcher_add(function(sammy) { ... }) call, plus a couple of other
// top-level statements (NAVIGATION/HELP entries) that need a minimal shape
// to not throw. None of that touches the DOM, so three small stubs are
// enough to load the file.
function loadFederationModule() {
  let dispatcherCallback;
  const sandbox = {
    dispatcher_add: (fn) => { dispatcherCallback = fn; },
    NAVIGATION: { Admin: [{}] },
    HELP: {}
  };
  vm.createContext(sandbox);
  vm.runInContext(federationSrc, sandbox, { filename: FEDERATION_JS_PATH });
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

describe('federation.js route registration', () => {
  it('registers its routes through dispatcher_add', () => {
    const { dispatcherCallback } = loadFederationModule();
    assert.equal(typeof dispatcherCallback, 'function');
  });

  it('registers exactly the expected methods and paths', () => {
    const { dispatcherCallback } = loadFederationModule();
    const sammy = fakeSammy();

    dispatcherCallback(sammy);

    assert.deepEqual(
      sammy.routes.map(({ method, path }) => [method, path]),
      [
        ['GET', '#/federation'],
        ['GET', '#/federation-upstreams'],
        ['GET', '#/federation-upstreams/:vhost/:id'],
        ['PUT', '#/fed-parameters'],
        ['DELETE', '#/fed-parameters'],
        ['DELETE', '#/federation-restart-link']
      ]
    );
  });

  it('registers a handler function for every route', () => {
    const { dispatcherCallback } = loadFederationModule();
    const sammy = fakeSammy();

    dispatcherCallback(sammy);

    assert.ok(sammy.routes.length > 0);
    for (const { method, path, handler } of sammy.routes) {
      assert.equal(typeof handler, 'function', `${method} ${path} should register a function`);
    }
  });
});
