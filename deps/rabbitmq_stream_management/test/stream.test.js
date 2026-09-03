import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const STREAM_JS_PATH = path.join(__dirname, '../priv/www/js/stream.js');
const streamSrc = fs.readFileSync(STREAM_JS_PATH, 'utf8');

// stream.js registers its routes at load time via a single top-level
// dispatcher_add(function(sammy) { ... }) call. Unlike shovel.js/
// federation.js, that same callback also pushes queue-page extension
// hooks onto QUEUE_EXTRA_CONTENT(_REQUESTS), and the file has several
// other top-level statements (NAVIGATION/COLUMNS/RENDER_CALLBACKS/
// CONSUMER_OWNER_FORMATTERS) that need a minimal shape to not throw.
// None of that touches the DOM.
function loadStreamModule() {
  let dispatcherCallback;
  const sandbox = {
    dispatcher_add: (fn) => { dispatcherCallback = fn; },
    NAVIGATION: {},
    COLUMNS: {},
    disable_stats: false,
    RENDER_CALLBACKS: {},
    QUEUE_EXTRA_CONTENT_REQUESTS: [],
    QUEUE_EXTRA_CONTENT: [],
    CONSUMER_OWNER_FORMATTERS: [],
    CONSUMER_OWNER_FORMATTERS_COMPARATOR: (a, b) => (a.order || 0) - (b.order || 0)
  };
  vm.createContext(sandbox);
  vm.runInContext(streamSrc, sandbox, { filename: STREAM_JS_PATH });
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

describe('stream.js route registration', () => {
  it('registers its routes through dispatcher_add', () => {
    const { dispatcherCallback } = loadStreamModule();
    assert.equal(typeof dispatcherCallback, 'function');
  });

  it('registers exactly the expected methods and paths', () => {
    const { dispatcherCallback } = loadStreamModule();
    const sammy = fakeSammy();

    dispatcherCallback(sammy);

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

  it('registers a handler function for every route', () => {
    const { dispatcherCallback } = loadStreamModule();
    const sammy = fakeSammy();

    dispatcherCallback(sammy);

    assert.ok(sammy.routes.length > 0);
    for (const { method, path, handler } of sammy.routes) {
      assert.equal(typeof handler, 'function', `${method} ${path} should register a function`);
    }
  });

  it('also registers the queue-page stream-publishers extension hooks', () => {
    const { sandbox, dispatcherCallback } = loadStreamModule();

    dispatcherCallback(fakeSammy());

    assert.equal(sandbox.QUEUE_EXTRA_CONTENT_REQUESTS.length, 1);
    assert.equal(sandbox.QUEUE_EXTRA_CONTENT.length, 1);
    assert.equal(typeof sandbox.QUEUE_EXTRA_CONTENT_REQUESTS[0], 'function');
    assert.equal(typeof sandbox.QUEUE_EXTRA_CONTENT[0], 'function');
  });
});
