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

%% End of Key JWT fields

%%
%% Rich Authorization Request fields
%%
-define(RAR_ACTIONS_FIELD, <<"actions">>).
-define(RAR_LOCATIONS_FIELD, <<"locations">>).
-define(RAR_TYPE_FIELD, <<"type">>).

-define(RAR_CLUSTER_LOCATION_ATTRIBUTE, <<"cluster">>).
-define(RAR_VHOST_LOCATION_ATTRIBUTE, <<"vhost">>).
-define(RAR_QUEUE_LOCATION_ATTRIBUTE, <<"queue">>).
-define(RAR_EXCHANGE_LOCATION_ATTRIBUTE, <<"exchange">>).
-define(RAR_ROUTING_KEY_LOCATION_ATTRIBUTE, <<"routing-key">>).
-define(RAR_LOCATION_ATTRIBUTES, [?RAR_CLUSTER_LOCATION_ATTRIBUTE, ?RAR_VHOST_LOCATION_ATTRIBUTE,
  ?RAR_QUEUE_LOCATION_ATTRIBUTE, ?RAR_EXCHANGE_LOCATION_ATTRIBUTE, ?RAR_ROUTING_KEY_LOCATION_ATTRIBUTE]).

-define(RAR_ALLOWED_TAG_VALUES, [<<"monitoring">>, <<"administrator">>, <<"management">>, <<"policymaker">> ]).
-define(RAR_ALLOWED_ACTION_VALUES, [<<"read">>, <<"write">>, <<"configure">>, <<"monitoring">>,
  <<"administrator">>, <<"management">>, <<"policymaker">> ]).

%% end of Rich Authorization Request fields


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
  additional_scopes_key :: binary(),
  preferred_username_claims :: list(),
  scope_aliases :: undefined | map(),
  oauth_provider_id :: oauth_provider_id()
 }).

-type resource_server() :: #resource_server{}.
-type resource_server_id() :: binary() | list().
