%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stream_super_streams_mgmt).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0,
         web_ui/0]).
-export([init/2,
         to_json/2,
         content_types_provided/2,
         resource_exists/2,
         is_authorized/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/logger.hrl").

-define(DEFAULT_RPC_TIMEOUT, 30_000).

-define(BASIC_COLUMNS,
        ["vhost",
         "name"]).

-define(DEFAULT_SORT, ["vhost", "name"]).

dispatcher() ->
    [{"/stream/super-streams", ?MODULE, []},
     {"/stream/super-streams/:vhost", ?MODULE, []}].

web_ui() ->
    [].

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest,
     rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE),
     #context{}}.

content_types_provided(ReqData, Context) ->
    {[{<<"application/json">>, to_json}], ReqData, Context}.

resource_exists(ReqData, Context) ->
    %% just checking that the vhost requested exists
    {case rabbit_mgmt_util:all_or_one_vhost(ReqData, fun (_) -> [] end) of
         vhost_not_found -> false;
         _               -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    try
        Basic = do_super_streams_query(ReqData),
        Data = rabbit_mgmt_util:augment_resources(Basic, ?DEFAULT_SORT,
                                                  ?BASIC_COLUMNS, ReqData,
                                                  Context, fun augment/2),
        rabbit_mgmt_util:reply(Data, ReqData, Context)
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData,
                                         Context)
    end.

augment(Basic, _ReqData) ->
    Basic.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

do_super_streams_query(ReqData) ->
 lists:foldl(fun(E, Acc) ->
        case rabbit_stream_mgmt_util:find_super_stream_from_exchange(E) of 
            {error, not_super_stream} -> Acc;
            S -> Acc ++ [S]
        end end, [], exchanges(ReqData)).

exchanges(ReqData) ->
    [rabbit_mgmt_format:exchange(X) || X <- exchanges0(ReqData)].

exchanges0(ReqData) ->
    rabbit_mgmt_util:all_or_one_vhost(ReqData, fun rabbit_exchange:info_all/1).


