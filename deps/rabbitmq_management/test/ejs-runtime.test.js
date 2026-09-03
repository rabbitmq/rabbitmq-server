import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EJS } from '../priv/www/js/ejs-runtime.js';

describe('ejs-runtime: to_text', () => {
  it('returns empty string for null and undefined', () => {
    assert.equal(EJS.Scanner.to_text(null), '');
    assert.equal(EJS.Scanner.to_text(undefined), '');
  });

  it('formats Date objects using toDateString()', () => {
    const d = new Date('2026-01-01T00:00:00Z');
    assert.equal(EJS.Scanner.to_text(d), d.toDateString());
  });

  it('converts numbers, booleans, strings, and objects with toString() to text', () => {
    assert.equal(EJS.Scanner.to_text(123), '123');
    assert.equal(EJS.Scanner.to_text(true), 'true');
    assert.equal(EJS.Scanner.to_text('hello'), 'hello');
    assert.equal(EJS.Scanner.to_text({ toString: () => 'custom' }), 'custom');
  });
});
