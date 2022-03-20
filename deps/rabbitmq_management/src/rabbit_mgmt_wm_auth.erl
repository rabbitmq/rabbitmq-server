%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_auth).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

to_json(ReqData, Context) ->
    EnableUAA = application:get_env(rabbitmq_management, enable_uaa, false),
    Data = case EnableUAA of
               true ->
                   UAAClientId = application:get_env(rabbitmq_management, uaa_client_id, ""),
                   UAALocation = application:get_env(rabbitmq_management, uaa_location, ""),
                   case is_invalid([UAAClientId, UAALocation]) of
                       true ->
                           rabbit_log:warning("Disabling OAuth 2 authorization, relevant configuration settings are missing", []),
                           [{enable_uaa, false}, {uaa_client_id, <<>>}, {uaa_location, <<>>}];
                       false ->
                           [{enable_uaa, true},
                            {uaa_client_id, rabbit_data_coercion:to_binary(UAAClientId)},
                            {uaa_location, rabbit_data_coercion:to_binary(UAALocation)}]
                   end;
               false ->
                   [{enable_uaa, false}, {uaa_client_id, <<>>}, {uaa_location, <<>>}]
           end,
    rabbit_mgmt_util:reply(Data, ReqData, Context).

is_authorized(ReqData, Context) ->
    {true, ReqData, Context}.

is_invalid(List) ->
    lists:any(fun(V) -> V == "" end, List).
