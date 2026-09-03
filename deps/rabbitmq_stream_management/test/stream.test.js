import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  link_stream_conn,
  ALL_STREAM_CONNECTION_COLUMNS,
  DISABLED_STATS_STREAM_CONNECTION_COLUMNS
} from '../priv/www/js/stream.js';
import {
  dispatcher_modules,
  NAVIGATION,
  HELP,
  ALL_COLUMNS,
  RENDER_CALLBACKS,
  QUEUE_EXTRA_CONTENT_REQUESTS,
  QUEUE_EXTRA_CONTENT
} from './global.js';
import { CONSUMER_OWNER_FORMATTERS } from './formatters.js';

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

describe('stream.js route registration and helper functions', () => {
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
        ['GET', '#/stream-connections'],
        ['GET', '#/stream-connections/:name']
      ]
    );
  });

  it('populates HELP and NAVIGATION registries upon ESM import', () => {
    assert.ok(HELP['stream-publisher-count']);
    assert.ok(NAVIGATION['Explore'][0]['Stream Connections']);
  });

  it('formats stream connection link correctly', () => {
    const html = link_stream_conn('127.0.0.1:5552 -> 127.0.0.1:54321');
    assert.equal(
      html,
      '<a href="#/stream-connections/127.0.0.1%3A5552%20-%3E%20127.0.0.1%3A54321">127.0.0.1:5552 -&gt; 127.0.0.1:54321</a>'
    );
  });

  it('exports column definitions and registers in COLUMNS', () => {
    assert.ok(ALL_STREAM_CONNECTION_COLUMNS.overview);
    assert.ok(DISABLED_STATS_STREAM_CONNECTION_COLUMNS.overview);
    assert.equal(ALL_COLUMNS['streamConnections'], ALL_STREAM_CONNECTION_COLUMNS);
  });

  it('registers RENDER_CALLBACKS for streamConnections', () => {
    assert.equal(typeof RENDER_CALLBACKS['streamConnections'], 'function');
  });

  it('registers extension hooks for QUEUE_EXTRA_CONTENT', () => {
    assert.ok(QUEUE_EXTRA_CONTENT_REQUESTS.length > 0);
    assert.ok(QUEUE_EXTRA_CONTENT.length > 0);

    const reqFn = QUEUE_EXTRA_CONTENT_REQUESTS[QUEUE_EXTRA_CONTENT_REQUESTS.length - 1];
    const reqs = reqFn('/', 'q1');
    assert.equal(reqs.stream_publishers, '/queues/%2F/q1/stream-publishers');
    assert.equal(reqs.stream_consumers, '/queues/%2F/q1/stream-consumers');
  });

  it('formats consumer owners when connection name starts with stream: ', () => {
    const streamConsumer = {
      channel_details: { name: 'stream: 127.0.0.1:5552 -> 127.0.0.1:54321' },
      subscription_id: 42
    };

    const streamFormatterEntry = CONSUMER_OWNER_FORMATTERS.find(entry => entry.order === 20);
    assert.ok(streamFormatterEntry, 'Expected stream formatter entry with order 20');

    const formatted = streamFormatterEntry.formatter(streamConsumer);
    assert.ok(formatted.includes('href="#/stream-connections/127.0.0.1%3A5552%20-%3E%20127.0.0.1%3A54321"'));
    assert.ok(formatted.includes('(sub id: 42)'));
  });
});
