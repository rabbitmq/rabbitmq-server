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
const GLOBAL_JS_PATH = path.join(__dirname, '../priv/www/js/global.js');
const globalSrc = fs.readFileSync(GLOBAL_JS_PATH, 'utf8');

// global.js is a plain browser script full of jQuery/DOM-touching code, but
// none of that runs at load time (only inside function/method bodies that
// are called later) so the file loads cleanly in a bare vm context. Each
// test gets a fresh context: `class ProcessorRegistry` itself is a top-level
// `class` declaration, which (unlike `var`/`function`) never becomes a
// sandbox property, so the five real registries the file wires up
// (postProcessorRegistry, loginProcessorRegistry, ...) are used directly as
// the class-under-test instead of `new`-ing up a throwaway one.
let sandbox;

function loadGlobalModule() {
  sandbox = { console };
  vm.createContext(sandbox);
  vm.runInContext(globalSrc, sandbox, { filename: GLOBAL_JS_PATH });
  return sandbox;
}

// invokeUntilFailure's own return values ({ok:true}, and the exception
// branch's {ok:false, error}) are built by code running inside the vm
// context, so they belong to a different realm than this file's object
// literals and aren't reference-equal under assert's strict deepEqual.
// Assert on the fields instead. (A processor's *own* returned object passes
// through untouched and stays reference-equal, so those are fine as-is.)
function assertResult(result, expected) {
  assert.equal(result.ok, expected.ok);
  assert.equal(result.error, expected.error);
}

describe('ProcessorRegistry: register / unregister / clear / isRegistered', () => {
  let registry;

  beforeEach(() => {
    sandbox = loadGlobalModule();
    registry = sandbox.postProcessorRegistry;
  });

  it('registers a named processor and reports it as registered', () => {
    assert.equal(registry.register('a', () => {}), true);
    assert.equal(registry.isRegistered('a'), true);
  });

  it('rejects a duplicate name', () => {
    registry.register('a', () => {});
    assert.equal(registry.register('a', () => {}), false);
  });

  it('rejects a null or undefined name', () => {
    assert.equal(registry.register(null, () => {}), false);
    assert.equal(registry.register(undefined, () => {}), false);
  });

  it('rejects a blank string name', () => {
    assert.equal(registry.register('', () => {}), false);
    assert.equal(registry.register('   ', () => {}), false);
  });

  it('rejects a non-function processor', () => {
    assert.equal(registry.register('a', 'not-a-function'), false);
    assert.equal(registry.isRegistered('a'), false);
  });

  it('accepts a non-string name as long as it is not null/undefined', () => {
    assert.equal(registry.register(42, () => {}), true);
    assert.equal(registry.isRegistered(42), true);
  });

  it('removes a registered processor', () => {
    registry.register('a', () => {});
    assert.equal(registry.unregister('a'), true);
    assert.equal(registry.isRegistered('a'), false);
  });

  it('returns false when unregistering a name that was never registered', () => {
    assert.equal(registry.unregister('missing'), false);
  });

  it('clear() removes every registered processor', () => {
    registry.register('a', () => {});
    registry.register('b', () => {});
    registry.clear();
    assert.equal(registry.isRegistered('a'), false);
    assert.equal(registry.isRegistered('b'), false);
  });
});

describe('ProcessorRegistry.invoke (non-blocking)', () => {
  let registry;

  beforeEach(() => {
    sandbox = loadGlobalModule();
    registry = sandbox.postProcessorRegistry;
  });

  it('calls every registered processor with the given data', () => {
    const calls = [];
    registry.register('a', (data) => calls.push(['a', data]));
    registry.register('b', (data) => calls.push(['b', data]));

    registry.invoke('payload');

    assert.deepEqual(calls, [['a', 'payload'], ['b', 'payload']]);
  });

  it('keeps calling later processors even if an earlier one throws', () => {
    const calls = [];
    registry.register('a', () => { throw new Error('boom'); });
    registry.register('b', () => calls.push('b'));

    registry.invoke();

    assert.deepEqual(calls, ['b']);
  });

  it('does nothing and does not throw when no processor is registered', () => {
    assert.doesNotThrow(() => registry.invoke('payload'));
  });
});

describe('ProcessorRegistry.invokeUntilFailure (short-circuiting)', () => {
  let registry;

  beforeEach(() => {
    sandbox = loadGlobalModule();
    registry = sandbox.loginProcessorRegistry;
  });

  it('returns ok:true after running every processor when none fail', () => {
    const calls = [];
    registry.register('a', () => { calls.push('a'); return { ok: true }; });
    registry.register('b', () => { calls.push('b'); });

    assertResult(registry.invokeUntilFailure(), { ok: true });
    assert.deepEqual(calls, ['a', 'b']);
  });

  it('stops at the first processor that returns ok:false, skipping the rest', () => {
    const calls = [];
    registry.register('a', () => { calls.push('a'); return { ok: false, error: 'nope' }; });
    registry.register('b', () => calls.push('b'));

    assertResult(registry.invokeUntilFailure(), { ok: false, error: 'nope' });
    assert.deepEqual(calls, ['a']);
  });

  it('stops and reports a failure when a processor throws', () => {
    const calls = [];
    registry.register('a', () => { throw new Error('boom'); });
    registry.register('b', () => calls.push('b'));

    assertResult(
      registry.invokeUntilFailure(),
      { ok: false, error: 'LoginProcessor a failed due to exception' }
    );
    assert.deepEqual(calls, []);
  });

  it('does not short-circuit on a truthy result that is not ok:false', () => {
    const calls = [];
    registry.register('a', () => { calls.push('a'); return 'ignored'; });
    registry.register('b', () => calls.push('b'));

    assertResult(registry.invokeUntilFailure(), { ok: true });
    assert.deepEqual(calls, ['a', 'b']);
  });

  it('returns ok:true immediately when no processor is registered', () => {
    assertResult(registry.invokeUntilFailure(), { ok: true });
  });

  it('passes the given data through to every processor', () => {
    const calls = [];
    registry.register('a', (data) => calls.push(data));
    registry.register('b', (data) => calls.push(data));

    registry.invokeUntilFailure('context-payload');

    assert.deepEqual(calls, ['context-payload', 'context-payload']);
  });
});

describe('global.js processor wrappers', () => {
  beforeEach(() => {
    sandbox = loadGlobalModule();
  });

  it('wires the postprocessor API to its own registry', () => {
    const calls = [];
    assert.equal(sandbox.registerPostProcessor('p', () => calls.push('p')), true);
    assert.equal(sandbox.is_postprocessor_registered('p'), true);

    sandbox.invokeRegisteredPostProcessors();
    assert.deepEqual(calls, ['p']);

    assert.equal(sandbox.unregisterPostProcessor('p'), true);
    sandbox.registerPostProcessor('q', () => calls.push('q'));
    sandbox.clear_postprocessors();
    assert.equal(sandbox.is_postprocessor_registered('q'), false);
  });

  it('wires the login processor API to invokeUntilFailure semantics', () => {
    sandbox.registerLoginProcessor('deny', () => ({ ok: false, error: 'denied' }));
    assertResult(sandbox.invokeLoginProcessors({}), { ok: false, error: 'denied' });

    assert.equal(sandbox.unregisterLoginProcessor('deny'), true);
    assertResult(sandbox.invokeLoginProcessors({}), { ok: true });
  });

  it('wires the init processor API to non-blocking invoke', () => {
    const calls = [];
    sandbox.registerInitProcessor('i', (ctx) => calls.push(ctx));
    sandbox.invokeInitProcessors('booted');
    assert.deepEqual(calls, ['booted']);
  });

  it('wires the init-failed processor API to non-blocking invoke', () => {
    const calls = [];
    sandbox.registerInitFailedProcessor('f', (err) => calls.push(err));
    sandbox.invokeInitFailedProcessors('boom');
    assert.deepEqual(calls, ['boom']);
  });

  it('wires the logout processor API to non-blocking invoke', () => {
    const calls = [];
    sandbox.registerLogoutProcessor('l', () => calls.push('logged-out'));
    sandbox.invokeLogoutProcessors();
    assert.deepEqual(calls, ['logged-out']);
  });

  it('keeps each processor category in its own registry', () => {
    // Registering the same name under two different categories must not collide.
    assert.equal(sandbox.registerPostProcessor('shared', () => {}), true);
    assert.equal(sandbox.registerLoginProcessor('shared', () => ({ ok: true })), true);
    assert.equal(sandbox.registerInitProcessor('shared', () => {}), true);
  });
});
