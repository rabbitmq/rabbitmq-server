%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% Matches the option type in rabbit_types without introducing a dependency
%% on that module and RabbitMQ core (rabbit_common)
-type(option(T) :: T | 'none' | 'undefined').

-type oauth_provider_id() :: root | binary().

-record(openid_configuration, {
    issuer :: option(uri_string:uri_string()),
    token_endpoint :: option(uri_string:uri_string()),
    authorization_endpoint :: option(uri_string:uri_string()),
    end_session_endpoint :: option(uri_string:uri_string()),
    jwks_uri :: option(uri_string:uri_string())
}).
-type openid_configuration() :: #openid_configuration{}.

-record(oauth_provider, {
    id :: oauth_provider_id(),
    issuer :: option(uri_string:uri_string()),
    discovery_endpoint :: option(uri_string:uri_string()),
    token_endpoint :: option(uri_string:uri_string()),
    tokeninfo_endpoint :: option(uri_string:uri_string()),
    authorization_endpoint :: option(uri_string:uri_string()),
    end_session_endpoint :: option(uri_string:uri_string()),
    jwks_uri :: option(uri_string:uri_string()),
    ssl_options :: option(list())
}).

-type query_list() :: [{unicode:chardata(), unicode:chardata() | true}].

-type oauth_provider() :: #oauth_provider{}.

-record(access_token_request, {
    client_id :: string() | binary(),
    client_secret :: string() | binary(),
    scope :: option(string() | binary()),
    extra_parameters :: option(query_list()),
    timeout :: option(integer())
}).

-type access_token_request() :: #access_token_request{}.

-record(successful_access_token_response, {
    access_token :: binary(),
    token_type :: binary(),
    %% Note: a refresh token SHOULD NOT be included
    %% ... for client-credentials flow.
    %% See https://www.rfc-editor.org/rfc/rfc6749#section-4.4.3
    refresh_token :: option(binary()),
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
