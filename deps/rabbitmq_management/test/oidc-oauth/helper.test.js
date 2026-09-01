import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

// helper.js touches window.location and window.localStorage; stub just
// enough of the browser global for oidc_settings_from to run under Node.
global.window = {
  localStorage: {},
  location: { protocol: 'https:', hostname: 'localhost', port: '', pathname: '/', hash: '' }
};

const { oidc_settings_from } = await import('../../priv/www/js/oidc-oauth/helper.js');

describe('oidc_settings_from', () => {
  describe('single root resource', () => {
    describe('with minimum required settings', () => {
      const resource = {
        oauth_client_id: 'some-client',
        oauth_provider_url: 'https://someurl',
        oauth_metadata_url: 'https://someurl/extra'
      };
      const oidc_settings = oidc_settings_from(resource);

      it('oidc_settings should have client_id', () => {
        assert.equal(oidc_settings.authority, resource.oauth_provider_url);
        assert.equal(oidc_settings.metadataUrl, resource.oauth_metadata_url);
        assert.equal(oidc_settings.client_id, resource.oauth_client_id);
      });
    });
  });
});
