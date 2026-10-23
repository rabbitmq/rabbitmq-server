// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const JS_PATH = path.join(__dirname, '../priv/www/js/terminology.js');

let pref;
let refreshed;
let navigationUpdates;
let requests;
const sandbox = {
  window: {},
  HELP: {
    'queued-messages': '<dt>Ready</dt><dd>x</dd><dt>Unacknowledged</dt><dd>for which the server is waiting for acknowledgement.</dd>',
    'message-rates': '<dt>Publish</dt><dd>p</dd> <dt>Get (empty)</dt> <dd>Rate at which empty queues are hit.</dd> <dt>Return</dt><dd>r</dd>',
    'other': 'Ready'
  },
  get_pref: () => pref,
  store_pref: (k, v) => { pref = v; },
  full_refresh: () => { refreshed = true; },
  disable_stats: false,
  authorization_header: () => 'Basic x',
  XMLHttpRequest: { DONE: 4 },
  update_navigation: () => { navigationUpdates++; },
  xmlHttpRequest: () => {
    const req = {
      open: (method, url) => { req.url = url; },
      setRequestHeader: () => {},
      send: () => { requests.push(req); }
    };
    return req;
  }
};
vm.createContext(sandbox);
vm.runInContext(fs.readFileSync(JS_PATH, 'utf8'), sandbox, { filename: JS_PATH });

beforeEach(() => {
  pref = null;
  refreshed = false;
  navigationUpdates = 0;
  requests = [];
  vm.runInContext('channels_seen = false; channels_check_in_flight = false;', sandbox);
  sandbox.disable_stats = false;
  sandbox.window.app_settings = undefined;
});

describe('terminology_mode', () => {
  it('defaults to modern', () => {
    assert.equal(sandbox.terminology_mode(), 'modern');
  });

  it('uses the configured default', () => {
    sandbox.window.app_settings = { ui_terminology: 'legacy' };
    assert.equal(sandbox.terminology_mode(), 'legacy');
  });

  it('prefers the user preference over the configured default', () => {
    sandbox.window.app_settings = { ui_terminology: 'legacy' };
    pref = 'modern';
    assert.equal(sandbox.terminology_mode(), 'modern');
  });

  it('ignores an unknown preference value', () => {
    pref = 'bogus';
    assert.equal(sandbox.terminology_mode(), 'modern');
  });
});

describe('t', () => {
  it('translates a legacy label in modern mode', () => {
    assert.equal(sandbox.t('Ready'), 'Available');
    assert.equal(sandbox.t('Unacked'), 'Acquired');
    assert.equal(sandbox.t('ack'), 'accept');
  });

  it('returns every label unchanged in legacy mode', () => {
    pref = 'legacy';
    for (const label of Object.keys(vm.runInContext('MODERN_TERMS', sandbox))) {
      assert.equal(sandbox.t(label), label);
    }
  });

  it('returns an unlisted label unchanged', () => {
    assert.equal(sandbox.t('Total'), 'Total');
    assert.equal(sandbox.t('constructor'), 'constructor');
  });
});

describe('t_items', () => {
  const items = [['Ready', 'messages_ready'],
                 ['Get (empty)', 'get_empty'],
                 ['Total', 'messages']];

  it('translates labels and drops basic.get rates in modern mode', () => {
    assert.equal(JSON.stringify(sandbox.t_items(items)),
                 JSON.stringify([['Available', 'messages_ready'], ['Total', 'messages']]));
  });

  it('keeps all items in legacy mode', () => {
    pref = 'legacy';
    assert.equal(JSON.stringify(sandbox.t_items(items)), JSON.stringify(items));
  });
});

describe('toggle_terminology', () => {
  it('switches mode and refreshes the page', () => {
    sandbox.toggle_terminology();
    assert.equal(pref, 'legacy');
    assert.ok(refreshed);
    sandbox.toggle_terminology();
    assert.equal(pref, 'modern');
  });
});

describe('help_text', () => {
  it('applies the modern edits', () => {
    assert.equal(sandbox.help_text('queued-messages'),
                 '<dt>Available</dt><dd>x</dd><dt>Acquired</dt><dd>that have been delivered and are waiting to be settled.</dd>');
  });

  it('removes the basic.get entries', () => {
    assert.equal(sandbox.help_text('message-rates'),
                 '<dt>Publish</dt><dd>p</dd> <dt>Return</dt><dd>r</dd>');
  });

  it('returns the legacy text in legacy mode', () => {
    pref = 'legacy';
    assert.equal(sandbox.help_text('queued-messages'), sandbox.HELP['queued-messages']);
  });

  it('returns an entry without edits unchanged', () => {
    assert.equal(sandbox.help_text('other'), 'Ready');
    assert.equal(sandbox.help_text('missing'), undefined);
  });
});

function respond(req, status, body) {
  req.readyState = 4;
  req.status = status;
  req.responseText = JSON.stringify(body);
  req.onreadystatechange();
}

describe('channels_visible', () => {
  it('is false in modern mode until a channel is seen', () => {
    assert.equal(sandbox.channels_visible(), false);
  });

  it('is always true in legacy mode', () => {
    pref = 'legacy';
    assert.equal(sandbox.channels_visible(), true);
  });
});

describe('check_channels_present', () => {
  it('shows channels once the API reports one', () => {
    sandbox.check_channels_present();
    assert.equal(requests.length, 1);
    respond(requests[0], 200, { items: [{}], total_count: 3 });
    assert.equal(sandbox.channels_visible(), true);
    assert.equal(navigationUpdates, 1);
  });

  it('keeps channels hidden when there are none', () => {
    sandbox.check_channels_present();
    respond(requests[0], 200, { items: [], total_count: 0 });
    assert.equal(sandbox.channels_visible(), false);
    assert.equal(navigationUpdates, 0);
  });

  it('keeps channels hidden when the request fails', () => {
    sandbox.check_channels_present();
    respond(requests[0], 401, {});
    assert.equal(sandbox.channels_visible(), false);
  });

  it('sends one request at a time', () => {
    sandbox.check_channels_present();
    sandbox.check_channels_present();
    assert.equal(requests.length, 1);
  });

  it('does not send a request in legacy mode or when stats are disabled', () => {
    pref = 'legacy';
    sandbox.check_channels_present();
    pref = null;
    sandbox.disable_stats = true;
    sandbox.check_channels_present();
    assert.equal(requests.length, 0);
  });

  it('does not send a request once a channel has been seen', () => {
    sandbox.check_channels_present();
    respond(requests[0], 200, { items: [{}], total_count: 1 });
    sandbox.check_channels_present();
    assert.equal(requests.length, 1);
  });
});
