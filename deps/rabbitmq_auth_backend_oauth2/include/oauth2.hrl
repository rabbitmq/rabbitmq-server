%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%


-include_lib("oauth2_client/include/oauth2_client.hrl").

-define(DEFAULT_PREFERRED_USERNAME_CLAIMS, [<<"sub">>, <<"client_id">>]).

-define(TOP_RESOURCE_SERVER_ID, application:get_env(?APP, resource_server_id)).
%% scope aliases map "role names" to a set of scopes

-record(internal_oauth_provider, {
    id :: oauth_provider_id(),
    default_key :: binary() | undefined,
    algorithms :: list() | undefined
}).
-type internal_oauth_provider() :: #internal_oauth_provider{}.

-record(resource_server, {
  id :: resource_server_id(),
  resource_server_type :: binary(),
  verify_aud :: boolean(),
  scope_prefix :: binary(),
  additional_scopes_key :: binary(),
  preferred_username_claims :: list(),
  scope_aliases :: undefined | map(),
  oauth_provider_id :: oauth_provider_id()
 }).

-type resource_server() :: #resource_server{}.
-type resource_server_id() :: binary() | list().
