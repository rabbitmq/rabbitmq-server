import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';

// Mock localStorage and window before importing prefs.js
const localStorageStore = new Map();
const mockLocalStorage = {
  getItem(key) {
    return localStorageStore.has(key) ? localStorageStore.get(key) : null;
  },
  setItem(key, value) {
    localStorageStore.set(key, String(value));
  },
  removeItem(key) {
    localStorageStore.delete(key);
  },
  clear() {
    localStorageStore.clear();
  }
};

globalThis.window = {
  localStorage: mockLocalStorage,
  COLUMNS: {
    queues: {
      general: [
        ['name', 'Name', true],
        ['memory', 'Memory', false]
      ]
    }
  }
};

import {
  CREDENTIALS,
  AUTH_SCHEME,
  SESSION_EXPIRY,
  AUTH_RESOURCE,
  BASIC_AUTH_SCHEME,
  BEARER_AUTH_SCHEME,
  DEFAULT_HARD_LOGIN_SESSION_TIMEOUT,
  set_auth_resource,
  has_auth_resource,
  get_auth_resource,
  clear_auth_resource,
  has_auth_credentials,
  get_auth_credentials,
  get_auth_scheme,
  clear_auth,
  set_token_auth,
  set_auth,
  set_session_expiry_if_required,
  authorization_header,
  store_local_pref,
  clear_local_pref,
  get_local_pref,
  store_pref,
  clear_pref,
  get_pref,
  section_pref,
  show_column,
  default_pref,
  default_column_pref
} from '../priv/www/js/prefs.js';

describe('prefs: local storage accessors', () => {
  beforeEach(() => {
    localStorageStore.clear();
  });

  it('stores, retrieves, and clears local preferences with prefix', () => {
    store_local_pref('foo', 'bar');
    assert.equal(get_local_pref('foo'), 'bar');
    assert.equal(mockLocalStorage.getItem('rabbitmq.foo'), 'bar');

    clear_local_pref('foo');
    assert.equal(get_local_pref('foo'), null);
  });

  it('store_pref and clear_pref act as aliases for local preferences', () => {
    store_pref('theme', 'dark');
    assert.equal(get_pref('theme'), 'dark');

    clear_pref('theme');
    assert.equal(get_pref('theme'), null);
  });

  it('get_pref returns explicit defaultValue when pref is absent', () => {
    assert.equal(get_pref('nonexistent', 'fallback'), 'fallback');
  });

  it('get_pref falls back to default_pref when no explicit default given', () => {
    assert.equal(get_pref('truncate'), '100');
    assert.equal(get_pref('chart-size-nodes'), 'small');
    assert.equal(get_pref('rate-mode-overview'), 'chart');
    assert.equal(get_pref('chart-line-queues'), 'true');
    assert.equal(get_pref('chart-range'), '60|5');
    assert.equal(get_pref('oauth-return-to'), '');
  });
});

describe('prefs: auth resource management', () => {
  beforeEach(() => {
    localStorageStore.clear();
  });

  it('sets, gets, checks, and clears auth resource', () => {
    assert.equal(has_auth_resource(), false);
    assert.equal(get_auth_resource(), null);

    set_auth_resource('rabbit_prod');
    assert.equal(has_auth_resource(), true);
    assert.equal(get_auth_resource(), 'rabbit_prod');

    clear_auth_resource();
    assert.equal(has_auth_resource(), false);
    assert.equal(get_auth_resource(), null);
  });
});

describe('prefs: authentication credentials & scheme', () => {
  beforeEach(() => {
    localStorageStore.clear();
  });

  it('sets and retrieves basic auth credentials', () => {
    set_auth('Basic', 'guest:guest');
    assert.equal(get_auth_scheme(), 'Basic');
    assert.equal(get_auth_credentials(), 'guest:guest');
    assert.equal(has_auth_credentials(), true);
    assert.equal(has_auth_credentials('Basic'), true);
    assert.equal(has_auth_credentials('Bearer'), false);
    assert.equal(authorization_header(), 'Basic guest:guest');
  });

  it('sets and retrieves bearer token auth', () => {
    set_token_auth('my-jwt-token');
    assert.equal(get_auth_scheme(), 'Bearer');
    assert.equal(get_auth_credentials(), 'my-jwt-token');
    assert.equal(has_auth_credentials('Bearer'), true);
    assert.equal(authorization_header(), 'Bearer my-jwt-token');
  });

  it('returns null authorization_header when unauthenticated', () => {
    assert.equal(authorization_header(), null);
    assert.equal(has_auth_credentials(), false);
  });

  it('clears all auth data', () => {
    set_auth('Basic', 'user:pass');
    set_auth_resource('res1');
    set_session_expiry_if_required(60);

    assert.equal(has_auth_credentials(), true);

    clear_auth();

    assert.equal(has_auth_credentials(), false);
    assert.equal(get_auth_credentials(), null);
    assert.equal(get_auth_scheme(), null);
    assert.equal(get_auth_resource(), null);
    assert.equal(get_local_pref(SESSION_EXPIRY), null);
  });
});

describe('prefs: session expiry', () => {
  beforeEach(() => {
    localStorageStore.clear();
  });

  it('sets default session expiry timeout when not specified', () => {
    const now = Date.now();
    set_session_expiry_if_required(undefined);

    const expiry = parseInt(get_local_pref(SESSION_EXPIRY), 10);
    assert.ok(expiry > now);
    const expectedMinutes = DEFAULT_HARD_LOGIN_SESSION_TIMEOUT;
    const diffMinutes = Math.round((expiry - now) / (60 * 1000));
    assert.equal(diffMinutes, expectedMinutes);
  });

  it('sets custom session expiry timeout in minutes', () => {
    const now = Date.now();
    set_session_expiry_if_required(30);

    const expiry = parseInt(get_local_pref(SESSION_EXPIRY), 10);
    const diffMinutes = Math.round((expiry - now) / (60 * 1000));
    assert.equal(diffMinutes, 30);
  });

  it('invalidates credentials when session is expired', () => {
    set_auth('Basic', 'user:pass');
    // Set expiry in the past
    store_local_pref(SESSION_EXPIRY, Date.now() - 1000);

    assert.equal(has_auth_credentials(), false);
    assert.equal(authorization_header(), null);
  });
});

describe('prefs: section & column helpers', () => {
  it('formats section preference string', () => {
    assert.equal(section_pref('overview', 'nodes'), 'visible|overview|nodes');
  });

  it('evaluates column visibility preferences', () => {
    assert.equal(show_column('queues', 'memory'), false);

    store_pref('column-queues-memory', 'true');
    assert.equal(show_column('queues', 'memory'), true);
  });

  it('looks up default column preferences from COLUMNS registry', () => {
    assert.equal(default_column_pref('queues-name'), 'true');
    assert.equal(default_column_pref('queues-nonexistent'), 'false');
  });
});
