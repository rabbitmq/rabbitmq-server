%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-include("types.hrl").

% define access token request common constants

-define(DEFAULT_HTTP_TIMEOUT, 60000).

% Refresh tome this number of seconds before expires_in token's attribute
-define(REFRESH_IN_BEFORE_EXPIRES_IN, 5).

-define(DEFAULT_OPENID_CONFIGURATION_PATH, "/.well-known/openid-configuration").

% define access token request constants
-define(CONTENT_URLENCODED, "application/x-www-form-urlencoded").
-define(CONTENT_JSON, "application/json").
-define(REQUEST_GRANT_TYPE, "grant_type").
-define(CLIENT_CREDENTIALS_GRANT_TYPE, "client_credentials").
-define(REFRESH_TOKEN_GRANT_TYPE, "refresh_token").

-define(REQUEST_CLIENT_ID, "client_id").
-define(REQUEST_CLIENT_SECRET, "client_secret").
-define(REQUEST_SCOPE, "scope").
-define(REQUEST_REFRESH_TOKEN, "refresh_token").

% define access token response constants
-define(BEARER_TOKEN_TYPE, <<"Bearer">>).

-define(RESPONSE_ACCESS_TOKEN, <<"access_token">>).
-define(RESPONSE_TOKEN_TYPE, <<"token_type">>).
-define(RESPONSE_EXPIRES_IN, <<"expires_in">>).
-define(RESPONSE_REFRESH_TOKEN, <<"refresh_token">>).

-define(RESPONSE_ERROR, <<"error">>).
-define(RESPONSE_ERROR_DESCRIPTION, <<"error_description">>).

-define(RESPONSE_ISSUER, <<"issuer">>).
-define(RESPONSE_TOKEN_ENDPOINT, <<"token_endpoint">>).
-define(RESPONSE_AUTHORIZATION_ENDPOINT, <<"authorization_endpoint">>).
-define(RESPONSE_END_SESSION_ENDPOINT, <<"end_session_endpoint">>).
-define(RESPONSE_JWKS_URI, <<"jwks_uri">>).
-define(RESPONSE_TLS_OPTIONS, <<"ssl_options">>).
