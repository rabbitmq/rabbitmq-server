import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  link_pid,
  fmt_sort_desc_by_default,
  fmt_process_name,
  fmt_remove_rabbit_prefix,
  fmt_pids,
  fmt_reduction_delta
} from '../priv/www/js/top.js';
import { dispatcher_modules, NAVIGATION, HELP } from './global.js';

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

describe('top.js route registration and formatters', () => {
  it('registers its routes through dispatcher_add', () => {
    assert.ok(dispatcher_modules.length > 0);
  });

  it('registers expected routes', () => {
    const sammy = fakeSammy();
    const registerFn = dispatcher_modules[dispatcher_modules.length - 1];
    registerFn(sammy);

    assert.deepEqual(
      sammy.routes.map(({ method, path }) => [method, path]),
      [
        ['GET', '#/top'],
        ['GET', '#/top/ets'],
        ['GET', '#/top/:node/:row_count'],
        ['GET', '#/top/ets/:node/:row_count'],
        ['GET', '#/process/:pid']
      ]
    );
  });

  it('populates HELP and NAVIGATION registries upon ESM import', () => {
    assert.ok(HELP['gen-server2-buffer']);
    assert.ok(NAVIGATION['Admin'][0]['Top Processes']);
    assert.ok(NAVIGATION['Admin'][0]['Top ETS Tables']);
  });

  it('formats pid links correctly', () => {
    const link = link_pid('<0.123.0>');
    assert.equal(link, '<a href="#/process/%3C0.123.0%3E">&lt;0.123.0&gt;</a>');
  });

  it('formats process names with rabbit_ prefix removal', () => {
    assert.equal(fmt_remove_rabbit_prefix('rabbit_amqqueue_process'), 'queue');
    assert.equal(fmt_remove_rabbit_prefix('rabbit_reader'), 'reader');
    assert.equal(fmt_remove_rabbit_prefix('other_process'), 'other_process');
  });

  it('formats process pids list', () => {
    const pids = fmt_pids(['<0.1.0>', '<0.2.0>']);
    assert.ok(pids.includes('#/process/%3C0.1.0%3E'));
    assert.ok(pids.includes('#/process/%3C0.2.0%3E'));
  });

  it('calculates reduction delta per second', () => {
    assert.equal(fmt_reduction_delta(100), 20);
    assert.equal(fmt_reduction_delta(12), 2);
  });
});
