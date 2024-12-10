const assert = require('assert')
import oidc_settings_from from '../../../../priv/www/js/oidc-oauth/helper.js'

describe('oidc_settings_from', function () {
  describe('single root resource', function () {
    
    describe('with minimum required settings', function () {
        var resource = { 
            oauth_client_id : "some-client",
            oauth_provider_url : "https://someurl",
            oauth_metadata_url : "https://someurl/extra"        
        }
        var oidc_settings = oidc_settings_from(resource)

        it('oidc_settings should have client_id ', function () {
            assert.equal(resource.oauth_provider_url, oidc_settings.authority)
            assert.equal(resource.oauth_metadata_url, oidc_settings.metadataUrl)
            assert.equal(resource.oauth_client_id, oidc_settings.client_id)
        })
    })
  })
})