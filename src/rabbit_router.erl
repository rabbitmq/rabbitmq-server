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

-behaviour(gen_server2).

-export([start_link/0,
         deliver/2,
         match_bindings/2,
         match_routing_key/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%% cross-node routing optimisation is disabled because of bug 19758.
-define(BUG19758, true).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> {'ok', pid()} | 'ignore' | {'error', any()}).
-spec(deliver/2 :: ([pid()], delivery()) -> {routing_result(), [pid()]}).

-endif.

%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

-ifdef(BUG19758).

deliver(QPids, Delivery) ->
    check_delivery(Delivery#delivery.mandatory, Delivery#delivery.immediate,
                   run_bindings(QPids, Delivery)).

-else.

deliver(QPids, Delivery) ->
    %% we reduce inter-node traffic by grouping the qpids by node and
    %% only delivering one copy of the message to each node involved,
    %% which then in turn delivers it to its queues.
    deliver_per_node(
      dict:to_list(
        lists:foldl(
          fun (QPid, D) ->
                  dict:update(node(QPid),
                              fun (QPids1) -> [QPid | QPids1] end,
                              [QPid], D)
          end,
          dict:new(), QPids)),
      Delivery).

deliver_per_node([{Node, QPids}], Delivery) when Node == node() ->
    %% optimisation
    check_delivery(Delivery#delivery.mandatory, Delivery#delivery.immediate,
                   run_bindings(QPids, Delivery));
deliver_per_node(NodeQPids, Delivery = #delivery{mandatory = false,
                                                 immediate = false}) ->
    %% optimisation: when Mandatory = false and Immediate = false,
    %% rabbit_amqqueue:deliver in run_bindings below will deliver the
    %% message to the queue process asynchronously, and return true,
    %% which means all the QPids will always be returned. It is
    %% therefore safe to use a fire-and-forget cast here and return
    %% the QPids - the semantics is preserved. This scales much better
    %% than the non-immediate case below.
    {routed,
     lists:flatmap(
       fun ({Node, QPids}) ->
               gen_server2:cast({?SERVER, Node}, {deliver, QPids, Delivery}),
               QPids
       end,
       NodeQPids)};
deliver_per_node(NodeQPids, Delivery) ->
    R = rabbit_misc:upmap(
          fun ({Node, QPids}) ->
                  try gen_server2:call({?SERVER, Node},
                                       {deliver, QPids, Delivery},
                                       infinity)
                  catch
                      _Class:_Reason ->
                          %% TODO: figure out what to log (and do!) here
                          {false, []}
                  end
          end,
          NodeQPids),
    {Routed, Handled} =
        lists:foldl(fun ({Routed, Handled}, {RoutedAcc, HandledAcc}) ->
                            {Routed or RoutedAcc,
                             %% we do the concatenation below, which
                             %% should be faster
                             [Handled | HandledAcc]}
                    end,
                    {false, []},
                    R),
    check_delivery(Delivery#delivery.mandatory, Delivery#delivery.immediate,
                   {Routed, lists:append(Handled)}).

-endif.

%% TODO: Maybe this should be handled by a cursor instead.
%% TODO: This causes a full scan for each entry with the same exchange
match_bindings(Name, Match) ->
    Query = qlc:q([QName || #route{binding = Binding = #binding{
                                               exchange_name = ExchangeName,
                                               queue_name = QName}} <-
                                mnesia:table(rabbit_route),
                            ExchangeName == Name,
                            Match(Binding)]),
    lookup_qpids(
      try
          mnesia:async_dirty(fun qlc:e/1, [Query])
      catch exit:{aborted, {badarg, _}} ->
              %% work around OTP-7025, which was fixed in R12B-1, by
              %% falling back on a less efficient method
              [QName || #route{binding = Binding = #binding{
                                           queue_name = QName}} <-
                            mnesia:dirty_match_object(
                              rabbit_route,
                              #route{binding = #binding{exchange_name = Name,
                                                        _ = '_'}}),
                        Match(Binding)]
      end).

match_routing_key(Name, RoutingKey) ->
    MatchHead = #route{binding = #binding{exchange_name = Name,
                                          queue_name = '$1',
                                          key = RoutingKey,
                                          _ = '_'}},
    lookup_qpids(mnesia:dirty_select(rabbit_route, [{MatchHead, [], ['$1']}])).

lookup_qpids(Queues) ->
    sets:fold(
      fun(Key, Acc) ->
              case mnesia:dirty_read({rabbit_queue, Key}) of
                  [#amqqueue{pid = QPid}] -> [QPid | Acc];
                  []                      -> Acc
              end
      end, [], sets:from_list(Queues)).

%%--------------------------------------------------------------------

init([]) ->
    {ok, no_state}.

handle_call({deliver, QPids, Delivery}, From, State) ->
    spawn(
      fun () ->
              R = run_bindings(QPids, Delivery),
              gen_server2:reply(From, R)
      end),
    {noreply, State}.

handle_cast({deliver, QPids, Delivery}, State) ->
    %% in order to preserve message ordering we must not spawn here
    run_bindings(QPids, Delivery),
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------

run_bindings(QPids, Delivery) ->
    lists:foldl(
      fun (QPid, {Routed, Handled}) ->
              case catch rabbit_amqqueue:deliver(QPid, Delivery) of
                  true              -> {true, [QPid | Handled]};
                  false             -> {true, Handled};
                  {'EXIT', _Reason} -> {Routed, Handled}
              end
      end,
      {false, []},
      QPids).

%% check_delivery(Mandatory, Immediate, {WasRouted, QPids})
check_delivery(true, _   , {false, []}) -> {unroutable, []};
check_delivery(_   , true, {_    , []}) -> {not_delivered, []};
check_delivery(_   , _   , {_    , Qs}) -> {routed, Qs}.
