import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { link_fed_conn } from '../priv/www/js/federation.js';
import { dispatcher_modules, HELP, NAVIGATION } from './global.js';

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
    assert.ok(dispatcher_modules.length > 0);
    assert.equal(typeof dispatcher_modules[dispatcher_modules.length - 1], 'function');
  });

  it('registers exactly the expected methods and paths', () => {
    const sammy = fakeSammy();
    const registerFn = dispatcher_modules[dispatcher_modules.length - 1];
    registerFn(sammy);

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

  it('populates HELP and NAVIGATION registries upon ESM import', () => {
    assert.ok(HELP['federation-uri']);
    assert.ok(NAVIGATION['Admin'][0]['Federation Status']);
  });

  it('formats federation connection link correctly', () => {
    const link = link_fed_conn('/', 'my-upstream');
    assert.equal(link, '<a href="#/federation-upstreams/%2F/my-upstream">my-upstream</a>');
  });
});
