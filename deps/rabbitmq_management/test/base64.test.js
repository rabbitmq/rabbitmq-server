import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { base64 } from '../priv/www/js/base64.js';

describe('base64: encoding and decoding', () => {
  it('encodes ASCII strings to base64', () => {
    assert.equal(base64.encode('hello'), 'aGVsbG8=');
    assert.equal(base64.encode('guest:guest'), 'Z3Vlc3Q6Z3Vlc3Q=');
    assert.equal(base64.encode(''), '');
  });

  it('decodes base64 strings to ASCII', () => {
    assert.equal(base64.decode('aGVsbG8='), 'hello');
    assert.equal(base64.decode('Z3Vlc3Q6Z3Vlc3Q='), 'guest:guest');
    assert.equal(base64.decode(''), '');
  });

  it('throws error when decoding invalid base64 input', () => {
    assert.throws(() => base64.decode('invalid_b64'), /Cannot decode base64/);
  });
});
