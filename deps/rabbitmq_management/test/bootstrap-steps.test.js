import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  registerLoginGate,
  unregisterLoginGate,
  registerInitStep,
  unregisterInitStep,
  clear_bootstrap_steps,
  bootstrap,
  unwind_active_steps
} from '../priv/www/js/bootstrap-steps.js';

function assertResult(result, expected) {
  assert.equal(result.ok, expected.ok);
  assert.equal(result.error, expected.error);
}

describe('registration', () => {
  beforeEach(() => { clear_bootstrap_steps(); });

  it('registers a login gate and an init step', () => {
    assert.equal(registerLoginGate('a', { run: () => {} }), true);
    assert.equal(registerInitStep('b', { run: () => {} }), true);
  });

  it('rejects a duplicate name within the same phase', () => {
    registerLoginGate('a', { run: () => {} });
    assert.equal(registerLoginGate('a', { run: () => {} }), false);
  });

  it('keeps the two phases in separate registries', () => {
    assert.equal(registerLoginGate('shared', { run: () => {} }), true);
    assert.equal(registerInitStep('shared', { run: () => {} }), true);
  });

  it('rejects a null or blank name', () => {
    assert.equal(registerLoginGate(null, { run: () => {} }), false);
    assert.equal(registerLoginGate('  ', { run: () => {} }), false);
  });

  it('rejects a step without a run function', () => {
    assert.equal(registerLoginGate('a', {}), false);
    assert.equal(registerLoginGate('a', { run: 'nope' }), false);
    assert.equal(registerLoginGate('a', null), false);
  });

  it('rejects a non-function rollback', () => {
    assert.equal(registerLoginGate('a', { run: () => {}, rollback: 'nope' }), false);
  });

  it('accepts a step with no rollback, since most init steps have nothing to undo', () => {
    assert.equal(registerInitStep('a', { run: () => {} }), true);
    assertResult(bootstrap({}), { ok: true, error: undefined });
  });

  it('unregisters a step', () => {
    registerLoginGate('a', { run: () => {} });
    assert.equal(unregisterLoginGate('a'), true);
    assert.equal(unregisterLoginGate('a'), false);
  });
});

describe('bootstrap: ordering and success', () => {
  beforeEach(() => { clear_bootstrap_steps(); });

  it('returns ok with an empty pipeline', () => {
    assertResult(bootstrap({}), { ok: true, error: undefined });
  });

  it('runs steps in registration order, gates before init steps', () => {
    const calls = [];
    registerInitStep('init1', { run: () => calls.push('init1') });
    registerLoginGate('gate1', { run: () => calls.push('gate1') });
    registerLoginGate('gate2', { run: () => calls.push('gate2') });
    registerInitStep('init2', { run: () => calls.push('init2') });

    bootstrap({});

    assert.deepEqual(calls, ['gate1', 'gate2', 'init1', 'init2']);
  });

  it('passes the context to every step', () => {
    const calls = [];
    registerLoginGate('gate', { run: (ctx) => calls.push(ctx) });
    registerInitStep('init', { run: (ctx) => calls.push(ctx) });

    bootstrap('the-context');

    assert.deepEqual(calls, ['the-context', 'the-context']);
  });

  it('does not roll anything back when everything succeeds', () => {
    const calls = [];
    registerLoginGate('gate', { run: () => {}, rollback: () => calls.push('gate') });
    registerInitStep('init', { run: () => {}, rollback: () => calls.push('init') });

    assertResult(bootstrap({}), { ok: true, error: undefined });
    assert.deepEqual(calls, []);
  });
});

describe('bootstrap: a login gate vetoes', () => {
  beforeEach(() => { clear_bootstrap_steps(); });

  it('stops at the vetoing gate and skips the rest', () => {
    const calls = [];
    registerLoginGate('a', { run: () => { calls.push('a'); } });
    registerLoginGate('b', { run: () => { calls.push('b'); return { ok: false, error: 'nope' }; } });
    registerLoginGate('c', { run: () => calls.push('c') });
    registerInitStep('init', { run: () => calls.push('init') });

    assertResult(bootstrap({}), { ok: false, error: 'nope' });
    assert.deepEqual(calls, ['a', 'b']);
  });

  it('rolls back every completed step, in reverse order', () => {
    const calls = [];
    registerLoginGate('a', { run: () => {}, rollback: () => calls.push('rollback-a') });
    registerLoginGate('b', { run: () => {}, rollback: () => calls.push('rollback-b') });
    registerLoginGate('c', { run: () => ({ ok: false, error: 'nope' }) });

    bootstrap({});

    assert.deepEqual(calls, ['rollback-b', 'rollback-a']);
  });

  it('does not roll back the step that vetoed', () => {
    const calls = [];
    registerLoginGate('a', { run: () => {}, rollback: () => calls.push('rollback-a') });
    registerLoginGate('b', {
      run: () => ({ ok: false, error: 'nope' }),
      rollback: () => calls.push('rollback-b')
    });

    bootstrap({});

    assert.deepEqual(calls, ['rollback-a']);
  });

  it('passes the context to rollbacks', () => {
    const calls = [];
    registerLoginGate('a', { run: () => {}, rollback: (ctx) => calls.push(ctx) });
    registerLoginGate('b', { run: () => ({ ok: false, error: 'nope' }) });

    bootstrap('the-context');

    assert.deepEqual(calls, ['the-context']);
  });

  it('supplies a caller default when the gate gives no reason', () => {
    registerLoginGate('a', { run: () => ({ ok: false }) });

    assertResult(bootstrap({}, 'the default'), { ok: false, error: 'the default' });
  });

  it('supplies a generic reason when there is no gate reason and no caller default', () => {
    registerLoginGate('a', { run: () => ({ ok: false }) });

    assertResult(bootstrap({}), { ok: false, error: 'LoginGate a rejected the request' });
  });

  it('does not treat a truthy non-{ok:false} return value as a veto', () => {
    const calls = [];
    registerLoginGate('a', { run: () => { calls.push('a'); return 'ignored'; } });
    registerLoginGate('b', { run: () => calls.push('b') });

    assertResult(bootstrap({}), { ok: true, error: undefined });
    assert.deepEqual(calls, ['a', 'b']);
  });
});

describe('bootstrap: a step throws', () => {
  beforeEach(() => { clear_bootstrap_steps(); });

  it('reports the failure rather than propagating the exception', () => {
    registerLoginGate('a', { run: () => { throw new Error('boom'); } });

    assertResult(bootstrap({}), { ok: false, error: 'LoginGate a failed due to exception' });
  });

  it('names the phase of the step that threw', () => {
    registerInitStep('a', { run: () => { throw new Error('boom'); } });

    assertResult(bootstrap({}), { ok: false, error: 'InitStep a failed due to exception' });
  });

  it('unwinds the login gates when an init step fails', () => {
    const calls = [];
    registerLoginGate('session', { run: () => {}, rollback: () => calls.push('rollback-session') });
    registerInitStep('extensions', { run: () => { throw new Error('boom'); } });

    assertResult(bootstrap({}), { ok: false, error: 'InitStep extensions failed due to exception' });
    assert.deepEqual(calls, ['rollback-session']);
  });

  it('keeps unwinding when a rollback itself throws', () => {
    const calls = [];
    registerLoginGate('a', { run: () => {}, rollback: () => calls.push('rollback-a') });
    registerLoginGate('b', { run: () => {}, rollback: () => { throw new Error('rollback boom'); } });
    registerLoginGate('c', { run: () => ({ ok: false, error: 'nope' }) });

    bootstrap({});

    assert.deepEqual(calls, ['rollback-a']);
  });
});

describe('unwind_active_steps', () => {
  beforeEach(() => { clear_bootstrap_steps(); });

  it('rolls back the steps of the last successful bootstrap, in reverse order', () => {
    const calls = [];
    registerLoginGate('session', { run: () => {}, rollback: () => calls.push('rollback-session') });
    registerInitStep('globals', { run: () => {}, rollback: () => calls.push('rollback-globals') });

    bootstrap({});
    assert.deepEqual(calls, []);

    unwind_active_steps({});

    assert.deepEqual(calls, ['rollback-globals', 'rollback-session']);
  });

  it('is a no-op when called twice', () => {
    const calls = [];
    registerLoginGate('session', { run: () => {}, rollback: () => calls.push('rollback-session') });

    bootstrap({});
    unwind_active_steps({});
    unwind_active_steps({});

    assert.deepEqual(calls, ['rollback-session']);
  });

  it('is a no-op when the last bootstrap failed, since it already unwound', () => {
    const calls = [];
    registerLoginGate('a', { run: () => {}, rollback: () => calls.push('rollback-a') });
    registerLoginGate('b', { run: () => ({ ok: false, error: 'nope' }) });

    bootstrap({});
    assert.deepEqual(calls, ['rollback-a']);

    unwind_active_steps({});

    assert.deepEqual(calls, ['rollback-a']);
  });
});

// A pipeline shaped like the real one: gates first, then the four init
// steps main.js registers. Every step records when it runs and when it is
// rolled back, so a failure injected at any position can be checked for
// exactly which steps ran and which were undone.
const PIPELINE = [
  ['gate', 'session'],
  ['gate', 'quota'],
  ['init', 'layout'],
  ['init', 'data-model'],
  ['init', 'events-and-refresh'],
  ['init', 'extensions']
];

function buildPipeline(failAt, mode) {
  const ran = [];
  const rolledBack = [];

  PIPELINE.forEach(([phase, name], index) => {
    const step = {
      run: () => {
        ran.push(name);
        if (index !== failAt) return;
        if (mode === 'throw') throw new Error(`${name} exploded`);
        return { ok: false, error: `${name} said no` };
      },
      rollback: () => rolledBack.push(name)
    };
    if (phase === 'gate') {
      registerLoginGate(name, step);
    } else {
      registerInitStep(name, step);
    }
  });

  return { ran, rolledBack };
}

const names = PIPELINE.map(([, name]) => name);

describe('bootstrap: a failure at each stage of a realistic pipeline', () => {
  beforeEach(() => { clear_bootstrap_steps(); });

  it('runs every step and rolls nothing back when all succeed', () => {
    const { ran, rolledBack } = buildPipeline(-1);

    assertResult(bootstrap({}), { ok: true, error: undefined });
    assert.deepEqual(ran, names);
    assert.deepEqual(rolledBack, []);
  });

  for (const mode of ['veto', 'throw']) {
    for (let failAt = 0; failAt < PIPELINE.length; failAt++) {
      const [phase, failing] = PIPELINE[failAt];
      const expectedRan = names.slice(0, failAt + 1);
      const expectedRolledBack = names.slice(0, failAt).reverse();
      const expectedError = mode === 'throw'
        ? `${phase === 'gate' ? 'LoginGate' : 'InitStep'} ${failing} failed due to exception`
        : `${failing} said no`;

      it(`${phase} "${failing}" ${mode}s: runs ${expectedRan.length}, unwinds ${expectedRolledBack.length}`, () => {
        const { ran, rolledBack } = buildPipeline(failAt, mode);

        assertResult(bootstrap({}), { ok: false, error: expectedError });
        assert.deepEqual(ran, expectedRan);
        assert.deepEqual(rolledBack, expectedRolledBack);
      });
    }
  }

  it('unwinds the login gates when the last init step fails', () => {
    const { rolledBack } = buildPipeline(PIPELINE.length - 1, 'throw');

    bootstrap({});

    assert.ok(rolledBack.includes('session'));
    assert.equal(rolledBack[rolledBack.length - 1], 'session');
  });

  it('leaves nothing for logout to unwind after a failed bootstrap', () => {
    const { rolledBack } = buildPipeline(3, 'throw');

    bootstrap({});
    const afterBootstrap = rolledBack.slice();
    unwind_active_steps({});

    assert.deepEqual(rolledBack, afterBootstrap);
  });

  it('leaves the whole pipeline for logout to unwind after a successful bootstrap', () => {
    const { rolledBack } = buildPipeline(-1);

    bootstrap({});
    unwind_active_steps({});

    assert.deepEqual(rolledBack, names.slice().reverse());
  });
});
