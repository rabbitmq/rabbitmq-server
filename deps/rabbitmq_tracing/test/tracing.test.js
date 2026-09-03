import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { link_trace, link_trace_queue } from '../priv/www/js/tracing.js';
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

describe('tracing.js route registration and formatters', () => {
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
        ['GET', '#/traces'],
        ['GET', '#/traces/:node'],
        ['GET', '#/traces/node/:node/:vhost/:name'],
        ['PUT', '#/traces/node/:node'],
        ['DELETE', '#/traces/node/:node'],
        ['DELETE', '#/trace-files/node/:node']
      ]
    );
  });

  it('populates HELP and NAVIGATION registries upon ESM import', () => {
    assert.ok(HELP['tracing-max-payload']);
    assert.ok(NAVIGATION['Admin'][0]['Tracing']);
  });

  it('formats trace file link correctly', () => {
    const link = link_trace('rabbit@node1', 'trace_file.log');
    assert.equal(
      link,
      '<a href="api/trace-files/node/rabbit%40node1/trace_file.log">trace_file.log</a>'
    );
  });

  it('formats trace queue link correctly', () => {
    const trace = { vhost: '/', queue: { name: 'q1' } };
    const link = link_trace_queue(trace);
    assert.equal(link, '<a href="#/queues/%2F/q1">(queue)</a>');
  });
});
