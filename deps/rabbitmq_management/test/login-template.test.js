import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';
import {
  authOptions,
  auth_options_for_mechanism,
  auth_section_is_expanded
} from '../priv/www/js/auth-options.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WWW_JS = path.join(__dirname, '../priv/www/js');

// Renders the real login_oauth.ejs through the original EJS compiler, the
// same way scripts/verify_ejs_templates.cjs does, but with real data rather
// than stubs. This guards the DOM contract the selenium page objects rely
// on (div#login-with-oauth2, div#login-with-basic-auth, select#oauth2-resource,
// form#basic-auth-form, and the section-visible/section-invisible classes),
// which unit tests of authOptions alone cannot cover.
const ejsSandbox = { window: {}, document: {} };
vm.createContext(ejsSandbox);
vm.runInContext(fs.readFileSync(path.join(WWW_JS, 'ejs-1.0.min.js'), 'utf8'), ejsSandbox);
// The template calls these as globals, so they must live in the realm the
// template is compiled in.
Object.assign(ejsSandbox, {
  authOptions,
  auth_options_for_mechanism,
  auth_section_is_expanded
});
ejsSandbox.fmt_string = (s) => String(s);

const templateSrc = fs.readFileSync(path.join(WWW_JS, 'tmpl/login_oauth.ejs'), 'utf8');

const DEV = { id: 'rabbit_dev', label: 'RabbitMQ Development' };
const PROD = { id: 'rabbit_prod', label: 'RabbitMQ Production' };

function render(oauthOverrides, renderOverrides) {
  const oauth = Object.assign({
    enabled: true,
    resource_servers: [],
    oauth_disable_basic_auth: false,
    strict_auth_mechanism: null,
    preferred_auth_mechanism: null
  }, oauthOverrides);

  const context = Object.assign({
    escaped_warnings: [],
    notAuthorized: false,
    auth: ejsSandbox.authOptions(oauth),
    auth_options_for_mechanism: ejsSandbox.auth_options_for_mechanism,
    auth_section_is_expanded: ejsSandbox.auth_section_is_expanded
  }, renderOverrides);

  return new ejsSandbox.EJS({ text: templateSrc, name: 'login_oauth' }).render(context);
}

function sectionClasses(html, id) {
  const match = new RegExp('class="([^"]*)"[^>]*id="' + id + '"').exec(html);
  return match ? match[1].trim() : null;
}

function directLoginResourceId(html) {
  const match = /data-oauth-action="login" data-resource-id="([^"]*)"/.exec(html);
  return match ? match[1] : null;
}

function selectedResource(html) {
  const match = /<option value="([^"]*)" selected="selected"/.exec(html);
  return match ? match[1] : null;
}

describe('login_oauth.ejs: which sections render', () => {
  it('offers both sections when oauth resources and basic auth are available', () => {
    const html = render({ resource_servers: [DEV, PROD] });
    assert.ok(sectionClasses(html, 'login-with-oauth2'));
    assert.ok(sectionClasses(html, 'login-with-basic-auth'));
    assert.ok(html.includes('Login with :'));
  });

  it('omits the oauth2 section when no resource is reachable', () => {
    // The IdPs-down configuration: the oauth section must be gone, and the
    // basic section must remain, so basic auth is still usable.
    const html = render({ resource_servers: [] });
    assert.equal(sectionClasses(html, 'login-with-oauth2'), null);
    assert.ok(sectionClasses(html, 'login-with-basic-auth'));
  });

  it('omits the basic section when basic auth is disabled', () => {
    const html = render({ resource_servers: [DEV, PROD], oauth_disable_basic_auth: true });
    assert.ok(sectionClasses(html, 'login-with-oauth2'));
    assert.equal(sectionClasses(html, 'login-with-basic-auth'), null);
  });

  it('renders a direct login button, with no section, for a single oauth resource', () => {
    const html = render({ resource_servers: [DEV], oauth_disable_basic_auth: true });
    assert.equal(directLoginResourceId(html), 'rabbit_dev');
    assert.equal(sectionClasses(html, 'login-with-oauth2'), null);
  });

  it('explains itself when no mechanism is available at all', () => {
    // Previously this combination matched no branch and rendered a bare logo.
    const html = render({ resource_servers: [], oauth_disable_basic_auth: true });
    assert.ok(html.includes('no authentication mechanism available'));
  });

  it('shows only warnings and a logout button when not authorized', () => {
    const html = render({ resource_servers: [DEV, PROD] },
                         { notAuthorized: true, escaped_warnings: ['Not authorized'] });
    assert.ok(html.includes('id="logout"'));
    assert.equal(sectionClasses(html, 'login-with-oauth2'), null);
    assert.equal(sectionClasses(html, 'login-with-basic-auth'), null);
  });
});

describe('login_oauth.ejs: which section is expanded', () => {
  it('expands oauth2 and collapses basic when nothing is preferred', () => {
    const html = render({ resource_servers: [DEV, PROD] });
    assert.ok(sectionClasses(html, 'login-with-oauth2').includes('section-visible'));
    assert.ok(sectionClasses(html, 'login-with-basic-auth').includes('section-invisible'));
  });

  it('emits exactly one visibility class per section', () => {
    // The old template could emit section-visible AND section-invisible on
    // the same div when basic auth was preferred.
    const html = render({ resource_servers: [DEV, PROD],
                          preferred_auth_mechanism: { type: 'basic' } });
    const oauth2 = sectionClasses(html, 'login-with-oauth2');
    const basic = sectionClasses(html, 'login-with-basic-auth');

    assert.ok(oauth2.includes('section-invisible'));
    assert.ok(!oauth2.includes('section-visible'));
    assert.ok(basic.includes('section-visible'));
    assert.ok(!basic.includes('section-invisible'));
  });

  it('leaves a lone basic section collapsed, so clicking its header expands it', () => {
    const html = render({ resource_servers: [] });
    assert.ok(sectionClasses(html, 'login-with-basic-auth').includes('section-invisible'));
  });

  it('expands the basic section when basic auth is strictly enforced', () => {
    const html = render({ resource_servers: [DEV, PROD],
                          strict_auth_mechanism: { type: 'basic' } });
    assert.ok(sectionClasses(html, 'login-with-basic-auth').includes('section-visible'));
  });
});

describe('login_oauth.ejs: resource selection', () => {
  it('renders a dropdown of every resource when there is more than one', () => {
    const html = render({ resource_servers: [DEV, PROD] });
    assert.ok(html.includes('id="oauth2-resource"'));
    assert.ok(html.includes('RabbitMQ Development'));
    assert.ok(html.includes('RabbitMQ Production'));
  });

  it('preselects the preferred resource in the dropdown', () => {
    const html = render({ resource_servers: [DEV, PROD],
                          preferred_auth_mechanism: { type: 'oauth2', resource_id: 'rabbit_dev' } });
    assert.equal(selectedResource(html), 'rabbit_dev');
  });

  it('preselects nothing when no resource is preferred', () => {
    const html = render({ resource_servers: [DEV, PROD] });
    assert.equal(selectedResource(html), null);
  });

  it('renders a direct button instead of a dropdown for a single resource', () => {
    const html = render({ resource_servers: [DEV] });
    assert.ok(!html.includes('id="oauth2-resource"'));
    assert.equal(directLoginResourceId(html), 'rabbit_dev');
  });

  it('honours a strictly enforced resource', () => {
    const html = render({ resource_servers: [DEV, PROD],
                          strict_auth_mechanism: { type: 'oauth2', resource_id: 'rabbit_prod' } });
    assert.equal(directLoginResourceId(html), 'rabbit_prod');
  });

  it('keeps the basic auth form fields the login flow posts', () => {
    const html = render({ resource_servers: [] });
    assert.ok(html.includes('id="basic-auth-form"'));
    assert.ok(html.includes('id="username"'));
    assert.ok(html.includes('id="password"'));
  });
});
