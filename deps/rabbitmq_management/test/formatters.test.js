// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FORMATTERS_JS_PATH = path.join(__dirname, '../priv/www/js/formatters.js');
const formattersSrc = fs.readFileSync(FORMATTERS_JS_PATH, 'utf8');

// formatters.js declares its own UNKNOWN_REPR and has no other top-level
// dependency, so it loads with no stubbing at all. It's loaded once and
// shared by every test below, since none of these functions hold state.
const sandbox = {};
vm.createContext(sandbox);
vm.runInContext(formattersSrc, sandbox, { filename: FORMATTERS_JS_PATH });

const UNKNOWN = '<span class="unknown">?</span>';

describe('fmt_si_prefix', () => {
  it('formats a value against its own magnitude when num equals max', () => {
    assert.equal(sandbox.fmt_si_prefix(1234, 1234, false, false), '1.2 k');
    assert.equal(sandbox.fmt_si_prefix(1234, 1234, true, false), '1.2 Ki');
  });

  it('scales every value on an axis by the axis max, not its own value', () => {
    // A small tick on a large-max axis keeps the axis's scale (M), not its own (none).
    assert.equal(sandbox.fmt_si_prefix(1200, 50_000_000, false, false), '0 M');
  });

  it('uses one decimal place only when allow_fractions is set and the scaled max is small', () => {
    // The trailing space is intentional: it separates the number from a unit
    // suffix appended by the caller (see fmt_bytes), and is present even
    // when there is no magnitude suffix (power 0).
    assert.equal(sandbox.fmt_si_prefix(3, 8, false, false), '3 ');
    assert.equal(sandbox.fmt_si_prefix(3, 8, false, true), '3.0 ');
  });

  it('formats zero the same way as any other value, with no special-casing needed', () => {
    // No dedicated zero branch: the general scaling path already produces
    // consistent output ("N ", matching every other magnitude) on its own.
    assert.equal(sandbox.fmt_si_prefix(0, 0, false, false), '0 ');
    assert.equal(sandbox.fmt_si_prefix(0, 20, false, true), '0 ');
  });
});

describe('fmt_bytes', () => {
  it('renders the unknown marker for undefined', () => {
    assert.equal(sandbox.fmt_bytes(undefined), UNKNOWN);
  });

  it('formats zero bytes consistently with non-zero values', () => {
    assert.equal(sandbox.fmt_bytes(0), '0 B');
  });

  it('picks a binary prefix for large values', () => {
    assert.equal(sandbox.fmt_bytes(52428800), '50 MiB');
  });
});

describe('fmt_uptime', () => {
  it('renders minutes and seconds under an hour', () => {
    assert.equal(sandbox.fmt_uptime(0), '0m 0s');
    assert.equal(sandbox.fmt_uptime(59999), '0m 59s');
  });

  it('switches to hours and minutes at the hour boundary', () => {
    assert.equal(sandbox.fmt_uptime(60000), '1m 0s');
    assert.equal(sandbox.fmt_uptime(3600000), '1h 0m');
  });

  it('switches to days and hours at the day boundary', () => {
    assert.equal(sandbox.fmt_uptime(86400000), '1d 0h');
    assert.equal(sandbox.fmt_uptime(90061000), '1d 1h');
  });
});

describe('short_conn', () => {
  it('returns the name unchanged when there is no peer arrow', () => {
    assert.equal(sandbox.short_conn('10.0.0.1:5672'), '10.0.0.1:5672');
  });

  it('keeps only the local side of a connection name', () => {
    assert.equal(sandbox.short_conn('10.0.0.1:5672 -> 10.0.0.2:41000'), '10.0.0.1:5672 ');
  });
});

describe('short_chan', () => {
  it('returns the name unchanged when it does not match the channel-number pattern', () => {
    assert.equal(sandbox.short_chan('plain-name'), 'plain-name');
  });

  it('keeps the local side plus the trailing channel number', () => {
    assert.equal(sandbox.short_chan('10.0.0.1:5672 -> 10.0.0.2:41000 (1)'), '10.0.0.1:5672  (1)');
  });
});

describe('fmt_num_thousands_unfixed', () => {
  it('leaves numbers under 1000 unchanged', () => {
    assert.equal(sandbox.fmt_num_thousands_unfixed('999'), '999');
  });

  it('groups digits in threes from the right', () => {
    assert.equal(sandbox.fmt_num_thousands_unfixed('1000'), '1,000');
    assert.equal(sandbox.fmt_num_thousands_unfixed('1234567'), '1,234,567');
  });
});

describe('fmt_escape_html0', () => {
  it('renders null and undefined as an empty string', () => {
    assert.equal(sandbox.fmt_escape_html0(null), '');
    assert.equal(sandbox.fmt_escape_html0(undefined), '');
  });

  it('escapes &, <, >, and " so the result is safe to inject as HTML', () => {
    assert.equal(
      sandbox.fmt_escape_html0('<a href="x">&"</a>'),
      '&lt;a href=&quot;x&quot;&gt;&amp;&quot;&lt;/a&gt;'
    );
  });
});

describe('fmt_rate_num', () => {
  it('renders the unknown marker for undefined', () => {
    assert.equal(sandbox.fmt_rate_num(undefined), UNKNOWN);
  });

  it('uses two decimal places under 1, one decimal place under 10', () => {
    assert.equal(sandbox.fmt_rate_num(0.456), '0.46');
    assert.equal(sandbox.fmt_rate_num(4.56), '4.6');
  });

  it('groups thousands with no decimal places at 10 and above', () => {
    assert.equal(sandbox.fmt_rate_num(1234), '1,234');
  });
});

describe('fmt_percent', () => {
  it('renders the unknown marker for undefined and empty string', () => {
    assert.equal(sandbox.fmt_percent(undefined), UNKNOWN);
    assert.equal(sandbox.fmt_percent(''), UNKNOWN);
  });

  it('renders a fraction as a rounded whole-number percentage', () => {
    assert.equal(sandbox.fmt_percent(0.4567), '46%');
  });
});

describe('fmt_boolean', () => {
  it('renders the unknown marker for undefined', () => {
    assert.equal(sandbox.fmt_boolean(undefined), UNKNOWN);
  });

  it('renders true and false as filled and empty circles', () => {
    assert.equal(sandbox.fmt_boolean(true), '&#9679;');
    assert.equal(sandbox.fmt_boolean(false), '&#9675;');
  });
});
