%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%


-include_lib("oauth2_client/include/oauth2_client.hrl").

-define(APP, rabbitmq_auth_backend_oauth2).
-define(DEFAULT_PREFERRED_USERNAME_CLAIMS, [<<"sub">>, <<"client_id">>]).
%% scope aliases map "role names" to a set of scopes

%%
%% Key JWT fields
%%

-define(AUD_JWT_FIELD, <<"aud">>).
-define(SCOPE_JWT_FIELD, <<"scope">>).
-define(TAG_SCOPE_PREFIX, <<"tag:">>).

%% End of Key JWT fields

%% UMA claim-type returns a RPT which is a token
%% where scopes are located under a map of list of objects which have 
%% the scopes in the "scopes" attribute
%% Used by Keycloak, WSO2 and others.
%% https://en.wikipedia.org/wiki/User-Managed_Access#cite_note-docs.wso2.com-19
-define(SCOPES_LOCATION_IN_REQUESTING_PARTY_TOKEN, <<"authorization.permissions.scopes">>).


-type raw_jwt_token() :: binary() | #{binary() => any()}.
-type decoded_jwt_token() :: #{binary() => any()}.

-record(internal_oauth_provider, {
    id :: oauth_provider_id(),
    default_key :: binary() | undefined,
    algorithms :: list() | undefined
}).
-type internal_oauth_provider() :: #internal_oauth_provider{}.

-record(resource_server, {
  id :: resource_server_id(),
  resource_server_type :: binary() | undefined,
  verify_aud :: boolean(),
  scope_prefix :: binary(),
  additional_scopes_key :: binary() | undefined,
  preferred_username_claims :: list(),
  scope_aliases :: map() | undefined,
  oauth_provider_id :: oauth_provider_id()
 }).

-type resource_server() :: #resource_server{}.
-type resource_server_id() :: binary() | list().
