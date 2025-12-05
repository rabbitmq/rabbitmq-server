%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_user_queues).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2,
         resource_exists/2, basic/1]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

-define(BASIC_COLUMNS,
        ["vhost",
         "name",
         "node",
         "durable",
         "auto_delete",
         "exclusive",
         "owner_pid",
         "arguments",
         "type",
         "pid",
         "state"]).

-define(DEFAULT_SORT, ["vhost", "name"]).

%%--------------------------------------------------------------------

init(Req, _InitialState) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData, Context) ->
    %% just checking that the vhost requested exists
    {case rabbit_mgmt_util:all_or_one_vhost(ReqData, fun (_) -> [] end) of
         vhost_not_found -> false;
         _               -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    try
        Basic = basic_owner_and_vhost_filtered(ReqData, Context),
        Data = rabbit_mgmt_util:augment_resources(Basic, ?DEFAULT_SORT,
                                                  ?BASIC_COLUMNS, ReqData,
                                                  Context, augment()),
        rabbit_mgmt_util:reply(Data, ReqData, Context)
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData,
                                         Context)
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost(ReqData, Context).

%%--------------------------------------------------------------------
%% Exported functions

basic(ReqData) ->
    %% rabbit_nodes:list_running/1 is a potentially slow function that performs
    %% a cluster wide query with a reasonably long (10s) timeout.
    %% TODO: replace with faster approximate function
    Running = rabbit_nodes:list_running(),
    Ctx = #{running_nodes => Running},
    FmtQ = fun (Q) -> rabbit_mgmt_format:queue(Q, Ctx) end,
    User = rabbit_mgmt_util:id(user, ReqData),
    list_queues(ReqData, Running, FmtQ, FmtQ, User).

list_queues(ReqData, Running, FormatRunningFun, FormatDownFun, User) ->
    [begin
         Pid = amqqueue:get_pid(Q),
         %% only queues whose leader pid is a on a non running node
         %% are considered "down", all other states should be passed
         %% as they are and the queue type impl will decide how to
         %% emit them.
         case not rabbit_amqqueue:is_local_to_node_set(Pid, Running) of
             false ->
                 FormatRunningFun(Q);
             true ->
                 FormatDownFun(amqqueue:set_state(Q, down))
         end
     end || Q <- all_queues(ReqData, User)].


%%--------------------------------------------------------------------
%% Private helpers

augment() ->
    fun(Basic, ReqData) ->
            case rabbit_mgmt_util:disable_stats(ReqData) of
                false ->
                    Stats = case rabbit_mgmt_util:columns(ReqData) of
                                all -> basic;
                                _ -> detailed
                            end,
                    rabbit_mgmt_db:augment_queues(Basic,
                                                  rabbit_mgmt_util:range_ceil(ReqData),
                                                  Stats);
                true ->
                    Basic
            end
    end.

basic_owner_and_vhost_filtered(ReqData, Context) ->
    rabbit_mgmt_util:filter_vhost(basic(ReqData), ReqData, Context).

all_queues(ReqData, User) ->
    rabbit_mgmt_util:all_or_one_vhost(ReqData, fun (VHost) -> list_all_for_user(VHost, User) end).

list_all_for_user(VHost, User) ->
    All = rabbit_amqqueue:list_all(VHost),
    [Q || Q <- All,
        maps:get(user, amqqueue:get_options(Q)) =:= User].
