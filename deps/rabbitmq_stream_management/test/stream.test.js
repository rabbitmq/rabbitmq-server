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
        ['GET', '#/stream/connections'],
        ['GET', '#/stream/connections/:vhost/:name'],
        ['GET', '#/stream/super-streams'],
        ['PUT', '#/stream/super-streams']
      ]
    );
  });

  it('populates NAVIGATION registries upon ESM import', () => {
    assert.deepEqual(NAVIGATION['Stream Connections'], ['#/stream/connections', 'monitoring']);
    assert.deepEqual(NAVIGATION['Super Streams'], ['#/stream/super-streams', 'management']);
  });

  it('formats stream connection link correctly', () => {
    const html = link_stream_conn('/', '127.0.0.1:5552 -> 127.0.0.1:54321');
    assert.equal(
      html,
      '<a href="#/stream/connections/%2F/127.0.0.1%3A5552%20-%3E%20127.0.0.1%3A54321">127.0.0.1:5552 </a>'
    );
  });

  it('exports column definitions and registers in COLUMNS', () => {
    assert.ok(ALL_STREAM_CONNECTION_COLUMNS.Overview);
    assert.ok(DISABLED_STATS_STREAM_CONNECTION_COLUMNS.Overview);
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
    assert.equal(reqs.extra_stream_publishers, '/stream/publishers/%2F/q1');
  });

  it('formats consumer owners when tag starts with stream.subid-', () => {
    const streamConsumer = {
      consumer_tag: 'stream.subid-123',
      queue: { vhost: '/' },
      channel_details: { connection_name: '127.0.0.1:5552 -> 127.0.0.1:54321' }
    };

    const streamFormatterEntry = CONSUMER_OWNER_FORMATTERS.find(entry => entry.order === 0);
    assert.ok(streamFormatterEntry, 'Expected stream formatter entry with order 0');

    const formatted = streamFormatterEntry.formatter(streamConsumer);
    assert.ok(formatted.includes('href="#/stream/connections/%2F/127.0.0.1%3A5552%20-%3E%20127.0.0.1%3A54321"'));
  });
});
