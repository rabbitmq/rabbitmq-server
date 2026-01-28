%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_shovel_index).

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([
    register_projection/0,
    shovels_by_source_queue/2,
    shovels_by_source_exchange/2,
    lookup/1
]).

-type shovel_key() :: {vhost:name(), ShovelName :: binary()}.

-export_type([shovel_key/0]).

%% ETS match patterns use '_' which violates declared field types.
-dialyzer({nowarn_function, [shovels_by_source_queue/2,
                             shovels_by_source_exchange/2]}).

-record(shovel_source_index, {
    key :: shovel_key(),
    source_type :: rabbit_shovel_definition:source_type(),
    source_protocol :: rabbit_shovel_definition:protocol(),
    source_queue :: rabbit_types:option(binary()),
    source_exchange :: rabbit_types:option(binary()),
    source_vhost :: vhost:name()
}).

-define(PROJECTION_NAME, rabbit_khepri_shovel_source_index).

%% @doc Register the Khepri projection for indexing shovels by source.
-spec register_projection() -> ok | {error, term()}.
register_projection() ->
    PathPattern = rabbit_db_rtparams:khepri_vhost_rp_path(
                    ?KHEPRI_WILDCARD_STAR_STAR,
                    <<"shovel">>,
                    ?KHEPRI_WILDCARD_STAR_STAR),
    MapFun = fun(_Path, #runtime_parameters{key = {VHost, <<"shovel">>, Name}, value = Def}) ->
                     extract_index_record(VHost, Name, Def)
             end,
    Options = #{keypos => #shovel_source_index.key},
    Projection = khepri_projection:new(?PROJECTION_NAME, MapFun, Options),
    khepri:register_projection(rabbit_khepri:get_store_id(), PathPattern, Projection).

-spec extract_index_record(vhost:name(), binary(), list() | map()) -> #shovel_source_index{}.
extract_index_record(VHost, Name, Def) ->
    %% Must use _for_index variant: Horus doesn't allow uri_string calls.
    SourceInfo = rabbit_shovel_definition:extract_source_info_for_index(Def, VHost),
    #{type := Type,
      protocol := Protocol,
      queue := Queue,
      vhost := SourceVHost} = SourceInfo,
    Exchange = maps:get(exchange, SourceInfo, undefined),
    #shovel_source_index{
       key = {VHost, Name},
       source_type = Type,
       source_protocol = Protocol,
       source_queue = Queue,
       source_exchange = Exchange,
       source_vhost = SourceVHost
      }.

%% @doc Find all shovels consuming from a specific queue.
-spec shovels_by_source_queue(vhost:name(), binary()) -> [shovel_key()].
shovels_by_source_queue(VHost, QueueName) ->
    try
        Match = #shovel_source_index{
            key = '_',
            source_type = queue,
            source_queue = QueueName,
            source_vhost = VHost,
            _ = '_'
        },
        [Key || #shovel_source_index{key = Key} <- ets:match_object(?PROJECTION_NAME, Match)]
    catch
        error:badarg -> []
    end.

%% @doc Find all shovels consuming from a specific exchange.
-spec shovels_by_source_exchange(vhost:name(), binary()) -> [shovel_key()].
shovels_by_source_exchange(VHost, ExchangeName) ->
    try
        Match = #shovel_source_index{
            key = '_',
            source_type = exchange,
            source_exchange = ExchangeName,
            source_vhost = VHost,
            _ = '_'
        },
        [Key || #shovel_source_index{key = Key} <- ets:match_object(?PROJECTION_NAME, Match)]
    catch
        error:badarg -> []
    end.

%% @doc Lookup a specific shovel's source info.
%%
%% Returns a map matching the structure of extract_source_info/2.
-spec lookup(shovel_key()) -> rabbit_shovel_definition:source_info() | undefined.
lookup(Key) ->
    try
        case ets:lookup(?PROJECTION_NAME, Key) of
            [#shovel_source_index{
                source_type = Type,
                source_protocol = Protocol,
                source_queue = Queue,
                source_exchange = Exchange,
                source_vhost = VHost
            }] ->
                BaseMap = #{type => Type,
                            protocol => Protocol,
                            queue => Queue,
                            vhost => VHost},
                case Exchange of
                    undefined -> BaseMap;
                    _ -> BaseMap#{exchange => Exchange}
                end;
            [] ->
                undefined
        end
    catch
        error:badarg -> undefined
    end.
