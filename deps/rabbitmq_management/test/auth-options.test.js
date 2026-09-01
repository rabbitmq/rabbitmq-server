import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const AUTH_OPTIONS_JS_PATH = path.join(__dirname, '../priv/www/js/auth-options.js');
const authOptionsSrc = fs.readFileSync(AUTH_OPTIONS_JS_PATH, 'utf8');

// auth-options.js is pure policy: no DOM, no dependencies, no state, so it
// is loaded once and shared by every test.
const sandbox = { console };
vm.createContext(sandbox);
vm.runInContext(authOptionsSrc, sandbox, { filename: AUTH_OPTIONS_JS_PATH });
const { authOptions, auth_section_is_expanded, auth_options_for_mechanism } = sandbox;

// authOptions builds its arrays inside the vm, so they belong to another
// realm and are never reference-equal under assert's strict deepEqual.
// Comparing a flattened string sidesteps that entirely.
function optionSummary(options) {
  return options.map((o) => `${o.mechanism}:${o.id}`).join(',');
}

const DEV = { id: 'rabbit_dev', label: 'RabbitMQ Development' };
const PROD = { id: 'rabbit_prod', label: 'RabbitMQ Production' };

function oauthSettings(overrides) {
  return Object.assign({
    enabled: true,
    resource_servers: [],
    oauth_disable_basic_auth: false,
    strict_auth_mechanism: null,
    preferred_auth_mechanism: null
  }, overrides);
}

// [description, settings, expected mode, expected [mechanism, id] pairs, expected preselected]
const MATRIX = [
  ['no oauth resources, basic disabled: nothing to log in with',
   { resource_servers: [], oauth_disable_basic_auth: true },
   'none', '', null],

  ['no oauth resources, basic enabled: basic only',
   { resource_servers: [], oauth_disable_basic_auth: false },
   'single', 'basic:basic', null],

  ['one oauth resource, basic disabled: that resource only',
   { resource_servers: [DEV], oauth_disable_basic_auth: true },
   'single', 'oauth2:rabbit_dev', null],

  ['one oauth resource, basic enabled: a choice of two',
   { resource_servers: [DEV], oauth_disable_basic_auth: false },
   'choice', 'oauth2:rabbit_dev,basic:basic', null],

  ['many oauth resources, basic disabled: a choice of resources',
   { resource_servers: [DEV, PROD], oauth_disable_basic_auth: true },
   'choice', 'oauth2:rabbit_dev,oauth2:rabbit_prod', null],

  ['many oauth resources, basic enabled: resources plus basic',
   { resource_servers: [DEV, PROD], oauth_disable_basic_auth: false },
   'choice', 'oauth2:rabbit_dev,oauth2:rabbit_prod,basic:basic', null],

  ['strict oauth2 with a resource id: only that resource, preselected',
   { resource_servers: [DEV, PROD],
     strict_auth_mechanism: { type: 'oauth2', resource_id: 'rabbit_prod' } },
   'single', 'oauth2:rabbit_prod', 'rabbit_prod'],

  ['strict basic: only basic, preselected',
   { resource_servers: [DEV, PROD], strict_auth_mechanism: { type: 'basic' } },
   'single', 'basic:basic', 'basic'],

  ['preferred oauth2 resource: full choice, that resource preselected',
   { resource_servers: [DEV, PROD],
     preferred_auth_mechanism: { type: 'oauth2', resource_id: 'rabbit_dev' } },
   'choice', 'oauth2:rabbit_dev,oauth2:rabbit_prod,basic:basic', 'rabbit_dev'],

  ['preferred basic: full choice, basic preselected',
   { resource_servers: [DEV, PROD], preferred_auth_mechanism: { type: 'basic' } },
   'choice', 'oauth2:rabbit_dev,oauth2:rabbit_prod,basic:basic', 'basic'],

  ['oauth disabled entirely: basic only',
   { enabled: false, resource_servers: [DEV, PROD] },
   'single', 'basic:basic', null]
];

describe('authOptions matrix', () => {
  for (const [description, overrides, expectedMode, expectedOptions, expectedPreselected] of MATRIX) {
    it(description, () => {
      const auth = authOptions(oauthSettings(overrides));
      assert.equal(auth.mode, expectedMode);
      assert.equal(optionSummary(auth.options), expectedOptions);
      assert.equal(auth.preselected, expectedPreselected);
    });
  }
});

describe('authOptions details', () => {
  it('falls back to the resource id when a resource has no label', () => {
    const auth = authOptions(oauthSettings({ resource_servers: [{ id: 'rabbit_dev' }] }));
    assert.equal(auth.options[0].label, 'rabbit_dev');
  });

  it('uses the resource label when present', () => {
    const auth = authOptions(oauthSettings({ resource_servers: [DEV] }));
    assert.equal(auth.options[0].label, 'RabbitMQ Development');
  });

  it('ignores a preference naming an option that was filtered out', () => {
    const auth = authOptions(oauthSettings({
      resource_servers: [DEV],
      oauth_disable_basic_auth: true,
      preferred_auth_mechanism: { type: 'basic' }
    }));
    assert.equal(auth.preselected, null);
  });

  it('treats strict oauth2 without a resource id as any oauth2 resource', () => {
    const auth = authOptions(oauthSettings({
      resource_servers: [DEV, PROD],
      strict_auth_mechanism: { type: 'oauth2' }
    }));
    assert.equal(auth.mode, 'choice');
    assert.equal(optionSummary(auth.options), 'oauth2:rabbit_dev,oauth2:rabbit_prod');
  });

  it('tolerates being called with no settings at all', () => {
    const auth = authOptions(undefined);
    assert.equal(auth.mode, 'single');
    assert.equal(optionSummary(auth.options), 'basic:basic');
  });
});

describe('auth_section_is_expanded', () => {
  it('expands oauth2 and collapses basic when nothing is preferred', () => {
    const auth = authOptions(oauthSettings({ resource_servers: [DEV, PROD] }));
    assert.equal(auth_section_is_expanded(auth, 'oauth2'), true);
    assert.equal(auth_section_is_expanded(auth, 'basic'), false);
  });

  it('expands basic and collapses oauth2 when basic is preferred', () => {
    const auth = authOptions(oauthSettings({
      resource_servers: [DEV, PROD],
      preferred_auth_mechanism: { type: 'basic' }
    }));
    // The old template emitted section-visible AND section-invisible on the
    // oauth2 div here, because its "visible" test compared the preference
    // object against the string "oauth2". Exactly one must apply.
    assert.equal(auth_section_is_expanded(auth, 'oauth2'), false);
    assert.equal(auth_section_is_expanded(auth, 'basic'), true);
  });

  it('expands oauth2 when a specific oauth2 resource is preferred', () => {
    const auth = authOptions(oauthSettings({
      resource_servers: [DEV, PROD],
      preferred_auth_mechanism: { type: 'oauth2', resource_id: 'rabbit_dev' }
    }));
    assert.equal(auth_section_is_expanded(auth, 'oauth2'), true);
    assert.equal(auth_section_is_expanded(auth, 'basic'), false);
  });

  it('leaves a lone basic section collapsed, since nothing was preferred', () => {
    // The IdP-down-with-basic-auth config: the section must render collapsed
    // so that clicking its header expands it.
    const auth = authOptions(oauthSettings({ resource_servers: [] }));
    assert.equal(auth.preselected, null);
    assert.equal(auth_section_is_expanded(auth, 'basic'), false);
  });

  it('expands a lone basic section when basic is strictly enforced', () => {
    const auth = authOptions(oauthSettings({ strict_auth_mechanism: { type: 'basic' } }));
    assert.equal(auth_section_is_expanded(auth, 'basic'), true);
  });
});

describe('auth_options_for_mechanism', () => {
  it('splits the options by mechanism, preserving order', () => {
    const auth = authOptions(oauthSettings({ resource_servers: [DEV, PROD] }));
    assert.equal(optionSummary(auth_options_for_mechanism(auth, 'oauth2')),
                 'oauth2:rabbit_dev,oauth2:rabbit_prod');
    assert.equal(optionSummary(auth_options_for_mechanism(auth, 'basic')), 'basic:basic');
  });
});
