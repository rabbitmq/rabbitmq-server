import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BOOTSTRAP_STEPS_JS_PATH = path.join(__dirname, '../priv/www/js/bootstrap-steps.js');
const bootstrapStepsSrc = fs.readFileSync(BOOTSTRAP_STEPS_JS_PATH, 'utf8');

// bootstrap-steps.js is a plain browser script with no dependencies beyond
// console, and it holds module-level registry state, so each test gets a
// fresh vm context.
let sandbox;

function loadBootstrapSteps() {
  sandbox = { console };
  vm.createContext(sandbox);
  vm.runInContext(bootstrapStepsSrc, sandbox, { filename: BOOTSTRAP_STEPS_JS_PATH });
  return sandbox;
}

// bootstrap()/run_phase() build their result objects inside the vm, so they
// belong to a different realm than this file's literals and are not
// reference-equal under assert's strict deepEqual. Assert on fields.
function assertResult(result, expected) {
  assert.equal(result.ok, expected.ok);
  assert.equal(result.error, expected.error);
}

describe('registration', () => {
  beforeEach(() => { loadBootstrapSteps(); });

  it('registers a login gate and an init step', () => {
    assert.equal(sandbox.registerLoginGate('a', { run: () => {} }), true);
    assert.equal(sandbox.registerInitStep('b', { run: () => {} }), true);
  });

  it('rejects a duplicate name within the same phase', () => {
    sandbox.registerLoginGate('a', { run: () => {} });
    assert.equal(sandbox.registerLoginGate('a', { run: () => {} }), false);
  });

  it('keeps the two phases in separate registries', () => {
    assert.equal(sandbox.registerLoginGate('shared', { run: () => {} }), true);
    assert.equal(sandbox.registerInitStep('shared', { run: () => {} }), true);
  });

  it('rejects a null or blank name', () => {
    assert.equal(sandbox.registerLoginGate(null, { run: () => {} }), false);
    assert.equal(sandbox.registerLoginGate('  ', { run: () => {} }), false);
  });

  it('rejects a step without a run function', () => {
    assert.equal(sandbox.registerLoginGate('a', {}), false);
    assert.equal(sandbox.registerLoginGate('a', { run: 'nope' }), false);
    assert.equal(sandbox.registerLoginGate('a', null), false);
  });

  it('rejects a non-function rollback', () => {
    assert.equal(sandbox.registerLoginGate('a', { run: () => {}, rollback: 'nope' }), false);
  });

  it('accepts a step with no rollback, since most init steps have nothing to undo', () => {
    assert.equal(sandbox.registerInitStep('a', { run: () => {} }), true);
    assertResult(sandbox.bootstrap({}), { ok: true, error: undefined });
  });

  it('unregisters a step', () => {
    sandbox.registerLoginGate('a', { run: () => {} });
    assert.equal(sandbox.unregisterLoginGate('a'), true);
    assert.equal(sandbox.unregisterLoginGate('a'), false);
  });
});

describe('bootstrap: ordering and success', () => {
  beforeEach(() => { loadBootstrapSteps(); });

  it('returns ok with an empty pipeline', () => {
    assertResult(sandbox.bootstrap({}), { ok: true, error: undefined });
  });

  it('runs steps in registration order, gates before init steps', () => {
    const calls = [];
    sandbox.registerInitStep('init1', { run: () => calls.push('init1') });
    sandbox.registerLoginGate('gate1', { run: () => calls.push('gate1') });
    sandbox.registerLoginGate('gate2', { run: () => calls.push('gate2') });
    sandbox.registerInitStep('init2', { run: () => calls.push('init2') });

    sandbox.bootstrap({});

    assert.deepEqual(calls, ['gate1', 'gate2', 'init1', 'init2']);
  });

  it('passes the context to every step', () => {
    const calls = [];
    sandbox.registerLoginGate('gate', { run: (ctx) => calls.push(ctx) });
    sandbox.registerInitStep('init', { run: (ctx) => calls.push(ctx) });

    sandbox.bootstrap('the-context');

    assert.deepEqual(calls, ['the-context', 'the-context']);
  });

  it('does not roll anything back when everything succeeds', () => {
    const calls = [];
    sandbox.registerLoginGate('gate', { run: () => {}, rollback: () => calls.push('gate') });
    sandbox.registerInitStep('init', { run: () => {}, rollback: () => calls.push('init') });

    assertResult(sandbox.bootstrap({}), { ok: true, error: undefined });
    assert.deepEqual(calls, []);
  });
});

describe('bootstrap: a login gate vetoes', () => {
  beforeEach(() => { loadBootstrapSteps(); });

  it('stops at the vetoing gate and skips the rest', () => {
    const calls = [];
    sandbox.registerLoginGate('a', { run: () => { calls.push('a'); } });
    sandbox.registerLoginGate('b', { run: () => { calls.push('b'); return { ok: false, error: 'nope' }; } });
    sandbox.registerLoginGate('c', { run: () => calls.push('c') });
    sandbox.registerInitStep('init', { run: () => calls.push('init') });

    assertResult(sandbox.bootstrap({}), { ok: false, error: 'nope' });
    assert.deepEqual(calls, ['a', 'b']);
  });

  it('rolls back every completed step, in reverse order', () => {
    const calls = [];
    sandbox.registerLoginGate('a', { run: () => {}, rollback: () => calls.push('rollback-a') });
    sandbox.registerLoginGate('b', { run: () => {}, rollback: () => calls.push('rollback-b') });
    sandbox.registerLoginGate('c', { run: () => ({ ok: false, error: 'nope' }) });

    sandbox.bootstrap({});

    assert.deepEqual(calls, ['rollback-b', 'rollback-a']);
  });

  it('does not roll back the step that vetoed', () => {
    const calls = [];
    sandbox.registerLoginGate('a', { run: () => {}, rollback: () => calls.push('rollback-a') });
    sandbox.registerLoginGate('b', {
      run: () => ({ ok: false, error: 'nope' }),
      rollback: () => calls.push('rollback-b')
    });

    sandbox.bootstrap({});

    assert.deepEqual(calls, ['rollback-a']);
  });

  it('passes the context to rollbacks', () => {
    const calls = [];
    sandbox.registerLoginGate('a', { run: () => {}, rollback: (ctx) => calls.push(ctx) });
    sandbox.registerLoginGate('b', { run: () => ({ ok: false, error: 'nope' }) });

    sandbox.bootstrap('the-context');

    assert.deepEqual(calls, ['the-context']);
  });

  it('supplies a caller default when the gate gives no reason', () => {
    sandbox.registerLoginGate('a', { run: () => ({ ok: false }) });

    assertResult(sandbox.bootstrap({}, 'the default'), { ok: false, error: 'the default' });
  });

  it('supplies a generic reason when there is no gate reason and no caller default', () => {
    sandbox.registerLoginGate('a', { run: () => ({ ok: false }) });

    assertResult(sandbox.bootstrap({}), { ok: false, error: 'LoginGate a rejected the request' });
  });

  it('does not treat a truthy non-{ok:false} return value as a veto', () => {
    const calls = [];
    sandbox.registerLoginGate('a', { run: () => { calls.push('a'); return 'ignored'; } });
    sandbox.registerLoginGate('b', { run: () => calls.push('b') });

    assertResult(sandbox.bootstrap({}), { ok: true, error: undefined });
    assert.deepEqual(calls, ['a', 'b']);
  });
});

describe('bootstrap: a step throws', () => {
  beforeEach(() => { loadBootstrapSteps(); });

  it('reports the failure rather than propagating the exception', () => {
    sandbox.registerLoginGate('a', { run: () => { throw new Error('boom'); } });

    assertResult(sandbox.bootstrap({}), { ok: false, error: 'LoginGate a failed due to exception' });
  });

  it('names the phase of the step that threw', () => {
    sandbox.registerInitStep('a', { run: () => { throw new Error('boom'); } });

    assertResult(sandbox.bootstrap({}), { ok: false, error: 'InitStep a failed due to exception' });
  });

  it('unwinds the login gates when an init step fails', () => {
    const calls = [];
    sandbox.registerLoginGate('session', { run: () => {}, rollback: () => calls.push('rollback-session') });
    sandbox.registerInitStep('extensions', { run: () => { throw new Error('boom'); } });

    assertResult(sandbox.bootstrap({}), { ok: false, error: 'InitStep extensions failed due to exception' });
    assert.deepEqual(calls, ['rollback-session']);
  });

  it('keeps unwinding when a rollback itself throws', () => {
    const calls = [];
    sandbox.registerLoginGate('a', { run: () => {}, rollback: () => calls.push('rollback-a') });
    sandbox.registerLoginGate('b', { run: () => {}, rollback: () => { throw new Error('rollback boom'); } });
    sandbox.registerLoginGate('c', { run: () => ({ ok: false, error: 'nope' }) });

    sandbox.bootstrap({});

    assert.deepEqual(calls, ['rollback-a']);
  });
});

describe('unwind_active_steps', () => {
  beforeEach(() => { loadBootstrapSteps(); });

  it('rolls back the steps of the last successful bootstrap, in reverse order', () => {
    const calls = [];
    sandbox.registerLoginGate('session', { run: () => {}, rollback: () => calls.push('rollback-session') });
    sandbox.registerInitStep('globals', { run: () => {}, rollback: () => calls.push('rollback-globals') });

    sandbox.bootstrap({});
    assert.deepEqual(calls, []);

    sandbox.unwind_active_steps({});

    assert.deepEqual(calls, ['rollback-globals', 'rollback-session']);
  });

  it('is a no-op when called twice', () => {
    const calls = [];
    sandbox.registerLoginGate('session', { run: () => {}, rollback: () => calls.push('rollback-session') });

    sandbox.bootstrap({});
    sandbox.unwind_active_steps({});
    sandbox.unwind_active_steps({});

    assert.deepEqual(calls, ['rollback-session']);
  });

  it('is a no-op when the last bootstrap failed, since it already unwound', () => {
    const calls = [];
    sandbox.registerLoginGate('a', { run: () => {}, rollback: () => calls.push('rollback-a') });
    sandbox.registerLoginGate('b', { run: () => ({ ok: false, error: 'nope' }) });

    sandbox.bootstrap({});
    assert.deepEqual(calls, ['rollback-a']);

    sandbox.unwind_active_steps({});

    assert.deepEqual(calls, ['rollback-a']);
  });
});
