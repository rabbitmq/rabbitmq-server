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
