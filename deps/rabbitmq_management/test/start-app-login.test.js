import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { shouldRunLoginAppAfterCheck } from '../priv/www/js/main.js';

// Derived from the pre-refactor start_app_login()'s branching on
// oauth.enabled: a successful check_login() hands off to
// finish_check_login()/start_app(), which replaces the login app entirely,
// so the router only needs restarting after a failed check, and only for
// mechanisms whose login UI actually needs it (basic's retry form; not
// oauth2's "not authorized" button).
describe('shouldRunLoginAppAfterCheck', () => {
  it('never restarts the router after a successful check, regardless of mechanism', () => {
    assert.equal(shouldRunLoginAppAfterCheck(true, true), false);
    assert.equal(shouldRunLoginAppAfterCheck(true, false), false);
  });

  it('restarts the router after a failed check when the mechanism needs it (basic)', () => {
    assert.equal(shouldRunLoginAppAfterCheck(false, true), true);
  });

  it('does not restart the router after a failed check when the mechanism does not need it (oauth2)', () => {
    assert.equal(shouldRunLoginAppAfterCheck(false, false), false);
  });
});
