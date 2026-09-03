import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  map,
  ALL_ARGS,
  NAVIGATION,
  HELP,
  expand_user_tags,
  registerPostProcessor,
  unregisterPostProcessor,
  clear_postprocessors,
  is_postprocessor_registered,
  invokeRegisteredPostProcessors,
  registerApplicationListener,
  unregisterApplicationListener,
  notifyOnRefresh,
  notifyOnVhostChange,
  notifyActivatedTab,
  set_current_user,
  get_current_user,
  set_current_vhost,
  get_current_vhost
} from '../priv/www/js/global.js';

describe('global: constants & arguments', () => {
  it('map utility transforms list to object with empty string values', () => {
    const result = map(['a', 'b']);
    assert.deepEqual(result, { a: '', b: '' });
  });

  it('ALL_ARGS combines IMPLICIT_ARGS and KNOWN_ARGS', () => {
    assert.equal(typeof ALL_ARGS['durable'], 'object');
    assert.equal(ALL_ARGS['durable'].short, 'D');
    assert.equal(ALL_ARGS['alternate-exchange'].short, 'AE');
  });

  it('NAVIGATION defines navigation sections', () => {
    assert.ok('Overview' in NAVIGATION);
    assert.ok('Connections' in NAVIGATION);
    assert.ok('Queues and Streams' in NAVIGATION);
  });

  it('HELP contains expected tooltip entries', () => {
    assert.ok('delivery-limit' in HELP);
    assert.ok('exchange-auto-delete' in HELP);
    assert.ok('queue-type' in HELP);
  });
});

describe('global: user & vhost state', () => {
  it('sets and gets current user', () => {
    set_current_user({ name: 'guest', tags: ['administrator'] });
    assert.deepEqual(get_current_user(), { name: 'guest', tags: ['administrator'] });
    set_current_user(null);
    assert.equal(get_current_user(), null);
  });

  it('sets and gets current vhost', () => {
    set_current_vhost('/myvhost');
    assert.equal(get_current_vhost(), '/myvhost');
  });
});

describe('global: expand_user_tags', () => {
  it('expands tag list including implicit tags based on hierarchy', () => {
    const tags = expand_user_tags(['administrator', 'custom_tag']);
    assert.ok(tags.includes('administrator'));
    assert.ok(tags.includes('monitoring'));
    assert.ok(tags.includes('policymaker'));
    assert.ok(tags.includes('management'));
    assert.ok(tags.includes('custom_tag'));
  });
});

describe('global: postprocessors', () => {
  beforeEach(() => {
    clear_postprocessors();
  });

  it('registers, checks, unregisters, and invokes postprocessors', () => {
    let invoked = false;
    const processor = () => { invoked = true; };

    assert.equal(is_postprocessor_registered('test_pp'), false);
    registerPostProcessor('test_pp', processor);
    assert.equal(is_postprocessor_registered('test_pp'), true);

    invokeRegisteredPostProcessors();
    assert.equal(invoked, true);

    unregisterPostProcessor('test_pp');
    assert.equal(is_postprocessor_registered('test_pp'), false);
  });
});

describe('global: application listeners', () => {
  it('registers and notifies application listeners', () => {
    const events = [];
    const listener = {
      onRefresh: () => events.push('refresh'),
      onVhostChange: (vhost) => events.push(`vhost:${vhost}`),
      onTabActivated: (tab) => events.push(`tab:${tab}`)
    };

    assert.equal(registerApplicationListener('test_listener', listener), true);
    assert.equal(registerApplicationListener('test_listener', listener), false);

    notifyOnRefresh();
    notifyOnVhostChange('my-vhost');
    notifyActivatedTab('#/queues');

    assert.deepEqual(events, ['refresh', 'vhost:my-vhost', 'tab:#/queues']);

    unregisterApplicationListener('test_listener');
  });
});
