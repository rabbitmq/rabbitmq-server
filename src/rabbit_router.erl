%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_router).
-include_lib("stdlib/include/qlc.hrl").
-include("rabbit.hrl").

-export([deliver/2,
         deliver_by_queue_names/2,
         match_bindings/2,
         match_routing_key/2]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([routing_key/0, routing_result/0]).

-type(routing_key() :: binary()).
-type(routing_result() :: 'routed' | 'unroutable' | 'not_delivered').
-type(qpids() :: [pid()]).

-spec(deliver/2 ::
        ([qpids()], rabbit_types:delivery()) -> {routing_result(), [qpids()]}).
-spec(deliver_by_queue_names/2 ::
        ([binary()], rabbit_types:delivery()) -> {routing_result(), [qpids()]}).
-spec(match_bindings/2 :: (rabbit_exchange:name(),
                           fun ((rabbit_types:binding()) -> boolean())) ->
    qpids()).
-spec(match_routing_key/2 :: (rabbit_exchange:name(), routing_key() | '_') ->
                                  qpids()).

-endif.

%%----------------------------------------------------------------------------

deliver(QPids, Delivery = #delivery{mandatory = false,
                                    immediate = false}) ->
    %% optimisation: when Mandatory = false and Immediate = false,
    %% rabbit_amqqueue:deliver will deliver the message to the queue
    %% process asynchronously, and return true, which means all the
    %% QPids will always be returned. It is therefore safe to use a
    %% fire-and-forget cast here and return the QPids - the semantics
    %% is preserved. This scales much better than the non-immediate
    %% case below.
    delegate:invoke_no_result(
      QPids, fun (Pid) -> rabbit_amqqueue:deliver(Pid, Delivery) end),
    {routed, QPids};

deliver(QPids, Delivery) ->
    {Success, _} =
        delegate:invoke(QPids,
                        fun (Pid) ->
                                rabbit_amqqueue:deliver(Pid, Delivery)
                        end),
    {Routed, Handled} =
        lists:foldl(fun fold_deliveries/2, {false, []}, Success),
    check_delivery(Delivery#delivery.mandatory, Delivery#delivery.immediate,
                   {Routed, Handled}).

deliver_by_queue_names(Qs, Delivery) ->
    deliver(lookup_qpids(Qs), Delivery).

%% TODO: Maybe this should be handled by a cursor instead.
%% TODO: This causes a full scan for each entry with the same exchange
match_bindings(Name, Match) ->
    Query = qlc:q([QName || #route{binding = Binding = #binding{
                                               exchange_name = XName,
                                               queue_name = QName}} <-
                                mnesia:table(rabbit_route),
                            XName == Name,
                            Match(Binding)]),
    lookup_qpids(mnesia:async_dirty(fun qlc:e/1, [Query])).

match_routing_key(Name, RoutingKey) ->
    MatchHead = #route{binding = #binding{exchange_name = Name,
                                          queue_name = '$1',
                                          key = RoutingKey,
                                          _ = '_'}},
    lookup_qpids(mnesia:dirty_select(rabbit_route, [{MatchHead, [], ['$1']}])).

lookup_qpids(Queues) ->
    lists:foldl(
      fun (Key, Acc) ->
              case mnesia:dirty_read({rabbit_queue, Key}) of
                  [#amqqueue{pid = QPid}] -> [QPid | Acc];
                  []                      -> Acc
              end
      end, [], lists:usort(Queues)).

%%--------------------------------------------------------------------

fold_deliveries({Pid, true},{_, Handled}) -> {true, [Pid|Handled]};
fold_deliveries({_,  false},{_, Handled}) -> {true, Handled}.

%% check_delivery(Mandatory, Immediate, {WasRouted, QPids})
check_delivery(true, _   , {false, []}) -> {unroutable, []};
check_delivery(_   , true, {_    , []}) -> {not_delivered, []};
check_delivery(_   , _   , {_    , Qs}) -> {routed, Qs}.
