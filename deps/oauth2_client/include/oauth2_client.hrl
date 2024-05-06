%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%


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
-define(RESPONSE_JWKS_URI, <<"jwks_uri">>).
-define(RESPONSE_TLS_OPTIONS, <<"ssl_options">>).

%% The closest we have to a type import in Erlang
-type option(T) :: rabbit_types:option(T).

-record(oauth_provider, {
  issuer :: option(uri_string:uri_string()),
  token_endpoint :: option(uri_string:uri_string()),
  authorization_endpoint :: option(uri_string:uri_string()),
  jwks_uri :: option(uri_string:uri_string()),
  ssl_options :: option(list())
  }).

-type oauth_provider() :: #oauth_provider{}.
-type oauth_provider_id() :: binary().

-record(access_token_request, {
  client_id :: string() | binary(),
  client_secret :: string() | binary(),
  scope :: string() | binary() | undefined,
  timeout :: option(integer())
  }).

-type access_token_request() :: #access_token_request{}.

-record(successful_access_token_response, {
  access_token :: binary(),
  token_type :: binary(),
  refresh_token :: option(binary()),    % A refresh token SHOULD NOT be included
                                        % .. for client-credentials flow.
                                        % https://www.rfc-editor.org/rfc/rfc6749#section-4.4.3
  expires_in :: option(integer())
}).

-type successful_access_token_response() :: #successful_access_token_response{}.

-record(unsuccessful_access_token_response, {
  error :: integer(),
  error_description :: binary() | string() | undefined
}).

-type unsuccessful_access_token_response() :: #unsuccessful_access_token_response{}.

-record(refresh_token_request, {
  client_id :: string() | binary(),
  client_secret :: string() | binary(),
  scope :: string() | binary() | undefined,
  refresh_token :: binary(),
  timeout :: option(integer())
  }).

-type refresh_token_request() :: #refresh_token_request{}.
