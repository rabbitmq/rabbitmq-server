import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  remove_params_with,
  rekey_params,
  link_shovel,
  fmt_shovel_endpoint,
  is_internal_shovel,
  shovel_has_internal_owner,
  shovel_internal_owner,
  fallback_value
} from '../priv/www/js/shovel.js';
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

describe('shovel.js route registration and helper functions', () => {
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
    const sammy = fakeSammy();
    const registerFn = dispatcher_modules[dispatcher_modules.length - 1];
    registerFn(sammy);

    assert.ok(sammy.routes.length > 0);
    for (const { method, path, handler } of sammy.routes) {
      assert.equal(typeof handler, 'function', `${method} ${path} should register a function`);
    }
  });

  it('populates HELP and NAVIGATION registries upon ESM import', () => {
    assert.ok(HELP['shovel-uri']);
    assert.ok(NAVIGATION['Admin'][0]['Shovel Status']);
  });

  it('remove_params_with deletes parameter keys matching prefix', () => {
    const sammy = { params: { 'amqp091-src-uri': 'amqp://', 'keep-me': 'yes' } };
    remove_params_with(sammy, 'amqp091-src');
    assert.deepEqual(sammy.params, { 'keep-me': 'yes' });
  });

  it('rekey_params transforms parameter keys using function', () => {
    const sammy = { params: { 'amqp10-src-uri': 'amqp://' } };
    rekey_params(sammy, (k) => k.replace('amqp10-', ''));
    assert.deepEqual(sammy.params, { 'src-uri': 'amqp://' });
  });

  it('link_shovel formats HTML link correctly', () => {
    const html = link_shovel('/', 'my-shovel');
    assert.equal(html, '<a href="#/dynamic-shovels/%2F/my-shovel">my-shovel</a>');
  });

  it('fmt_shovel_endpoint formats AMQP 1.0 vs AMQP 0.9.1 endpoints', () => {
    const shovel10 = { 'src-protocol': 'amqp10', 'src-address': 'queue/a' };
    assert.equal(fmt_shovel_endpoint('src-', shovel10), 'queue/a');

    const shovel091 = { 'src-protocol': 'amqp091', 'src-queue': 'q1' };
    assert.equal(fmt_shovel_endpoint('src-', shovel091), 'q1<sub>queue</sub>');
  });

  it('is_internal_shovel identifies internal shovels', () => {
    assert.equal(is_internal_shovel({ internal: true }), true);
    assert.equal(is_internal_shovel({}), false);
  });

  it('shovel_has_internal_owner & shovel_internal_owner identify owner', () => {
    const shovel = { internal_owner: 'owner1' };
    assert.equal(shovel_has_internal_owner(shovel), true);
    assert.equal(shovel_internal_owner(shovel), 'owner1');
    assert.equal(shovel_has_internal_owner({}), false);
  });

  it('fallback_value selects fallback key when primary key missing', () => {
    const shovel = { value: { key2: 'val2' } };
    assert.equal(fallback_value(shovel, 'key1', 'key2'), 'val2');
  });
});
