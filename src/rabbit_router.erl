%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_router).
-include_lib("stdlib/include/qlc.hrl").
-include("rabbit.hrl").

-export([deliver/2, match_bindings/2, match_routing_key/2]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([routing_key/0, routing_result/0, match_result/0]).

-type(routing_key() :: binary()).
-type(routing_result() :: 'routed' | 'unroutable' | 'not_delivered').
-type(qpids() :: [pid()]).
-type(match_result() :: [rabbit_types:binding_destination()]).

-spec(deliver/2 :: ([rabbit_amqqueue:name()], rabbit_types:delivery()) ->
                        {routing_result(), qpids()}).
-spec(match_bindings/2 :: (rabbit_types:binding_source(),
                           fun ((rabbit_types:binding()) -> boolean())) ->
    match_result()).
-spec(match_routing_key/2 :: (rabbit_types:binding_source(),
                             [routing_key()] | ['_']) ->
    match_result()).

-endif.

%%----------------------------------------------------------------------------

deliver(QNames, Delivery = #delivery{mandatory = false,
                                     immediate = false}) ->
    %% optimisation: when Mandatory = false and Immediate = false,
    %% rabbit_amqqueue:deliver will deliver the message to the queue
    %% process asynchronously, and return true, which means all the
    %% QPids will always be returned. It is therefore safe to use a
    %% fire-and-forget cast here and return the QPids - the semantics
    %% is preserved. This scales much better than the non-immediate
    %% case below.
    QPids = lookup_qpids(QNames),
    delegate:invoke_no_result(
      QPids, fun (Pid) -> rabbit_amqqueue:deliver(Pid, Delivery) end),
    {routed, QPids};

deliver(QNames, Delivery = #delivery{mandatory = Mandatory,
                                     immediate = Immediate}) ->
    QPids = lookup_qpids(QNames),
    {Success, _} =
        delegate:invoke(QPids,
                        fun (Pid) ->
                                rabbit_amqqueue:deliver(Pid, Delivery)
                        end),
    {Routed, Handled} =
        lists:foldl(fun fold_deliveries/2, {false, []}, Success),
    check_delivery(Mandatory, Immediate, {Routed, Handled}).


%% TODO: Maybe this should be handled by a cursor instead.
%% TODO: This causes a full scan for each entry with the same source
match_bindings(SrcName, Match) ->
    Query = qlc:q([DestinationName ||
                      #route{binding = Binding = #binding{
                                         source      = SrcName1,
                                         destination = DestinationName}} <-
                          mnesia:table(rabbit_route),
                      SrcName == SrcName1,
                      Match(Binding)]),
    mnesia:async_dirty(fun qlc:e/1, [Query]).

match_routing_key(SrcName, [RoutingKey]) ->
    MatchHead = #route{binding = #binding{source      = SrcName,
                                          destination = '$1',
                                          key         = RoutingKey,
                                          _           = '_'}},
    mnesia:dirty_select(rabbit_route, [{MatchHead, [], ['$1']}]);
match_routing_key(SrcName, [_|_] = RoutingKeys) ->
    Condition = list_to_tuple(['orelse' | [{'=:=', '$2', RKey} ||
                                              RKey <- RoutingKeys]]),
    MatchHead = #route{binding = #binding{source      = SrcName,
                                          destination = '$1',
                                          key         = '$2',
                                          _           = '_'}},
    mnesia:dirty_select(rabbit_route, [{MatchHead, [Condition], ['$1']}]).



%%--------------------------------------------------------------------

fold_deliveries({Pid, true},{_, Handled}) -> {true, [Pid|Handled]};
fold_deliveries({_,  false},{_, Handled}) -> {true, Handled}.

%% check_delivery(Mandatory, Immediate, {WasRouted, QPids})
check_delivery(true, _   , {false, []}) -> {unroutable, []};
check_delivery(_   , true, {_    , []}) -> {not_delivered, []};
check_delivery(_   , _   , {_    , Qs}) -> {routed, Qs}.

lookup_qpids(QNames) ->
    lists:foldl(fun (QName, QPids) ->
                        case mnesia:dirty_read({rabbit_queue, QName}) of
                            [#amqqueue{pid = QPid, mirror_pids = MPids}] ->
                                MPids ++ [QPid | QPids];
                            [] ->
                                QPids
                        end
                end, [], QNames).
