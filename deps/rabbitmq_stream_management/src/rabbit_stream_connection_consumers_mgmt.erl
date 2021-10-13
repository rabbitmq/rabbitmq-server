%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_connection_consumers_mgmt).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0,
         web_ui/0]).
-export([init/2,
         to_json/2,
         content_types_provided/2,
         is_authorized/2]).
-export([resource_exists/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

dispatcher() ->
    case rabbit_stream_management_utils:is_feature_flag_enabled() of
      true ->   [{"/stream/connections/:vhost/:connection/consumers", ?MODULE, []}];
      false -> []
    end.


web_ui() ->
    [].

%%--------------------------------------------------------------------
init(Req, _State) ->
    {cowboy_rest,
     rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE),
     #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
    {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData, Context) ->
    case rabbit_mgmt_wm_connection:conn(ReqData) of
        error ->
            {false, ReqData, Context};
        not_found ->
            {false, ReqData, Context};
        _Conn ->
            {true, ReqData, Context}
    end.

to_json(ReqData, Context) ->
    Pid = proplists:get_value(pid,
                              rabbit_mgmt_wm_connection:conn(ReqData)),
    Consumers =
        rabbit_mgmt_format:strip_pids(
            rabbit_stream_mgmt_db:get_connection_consumers(Pid)),
    rabbit_mgmt_util:reply_list(Consumers, ReqData, Context).

is_authorized(ReqData, Context) ->
    try
        rabbit_mgmt_util:is_authorized_user(ReqData, Context,
                                            rabbit_mgmt_wm_connection:conn(ReqData))
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData,
                                         Context)
    end.
