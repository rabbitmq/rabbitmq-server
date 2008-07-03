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
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial Technologies
%%   LLC., and Rabbit Technologies Ltd. are Copyright (C) 2007-2008
%%   LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_router).
-include("rabbit.hrl").

-behaviour(gen_server).

-export([start_link/0,
         deliver/5]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> {'ok', pid()} | 'ignore' | {'error', any()}).
-spec(deliver/5 :: ([pid()], bool(), bool(), maybe(txn()), message()) ->
             {'ok', [pid()]} | {'error', 'unroutable' | 'not_delivered'}).

-endif.

%%----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

deliver(QPids, Mandatory, Immediate, Txn, Message) ->
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
      Mandatory, Immediate, Txn, Message).

deliver_per_node([{Node, QPids}], Mandatory, Immediate,
                 Txn, Message)
  when Node == node() ->
    %% optimisation
    check_delivery(Mandatory, Immediate,
                   run_bindings(QPids, Mandatory, Immediate, Txn, Message));
deliver_per_node(NodeQPids, Mandatory = false, Immediate = false,
                 Txn, Message) ->
    %% optimisation: when Mandatory = false and Immediate = false,
    %% rabbit_amqqueue:deliver in run_bindings below will deliver the
    %% message to the queue process asynchronously, and return true,
    %% which means all the QPids will always be returned. It is
    %% therefore safe to use a fire-and-forget cast here and return
    %% the QPids - the semantics is preserved. This scales much better
    %% than the non-immediate case below.
    {ok, lists:flatmap(
           fun ({Node, QPids}) ->
                   gen_server:cast(
                     {?SERVER, Node},
                     {deliver, QPids, Mandatory, Immediate, Txn, Message}),
                   QPids
           end,
           NodeQPids)};
deliver_per_node(NodeQPids, Mandatory, Immediate,
                 Txn, Message) ->
    R = rabbit_misc:upmap(
          fun ({Node, QPids}) ->
                  try gen_server:call(
                        {?SERVER, Node},
                        {deliver, QPids, Mandatory, Immediate, Txn, Message})
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
    check_delivery(Mandatory, Immediate, {Routed, lists:append(Handled)}).

%%--------------------------------------------------------------------

init([]) ->
    {ok, no_state}.

handle_call({deliver, QPids, Mandatory, Immediate, Txn, Message},
            From, State) ->
    spawn(
      fun () ->
              R = run_bindings(QPids, Mandatory, Immediate, Txn, Message),
              gen_server:reply(From, R)
      end),
    {noreply, State}.

handle_cast({deliver, QPids, Mandatory, Immediate, Txn, Message},
            State) ->
    %% in order to preserve message ordering we must not spawn here
    run_bindings(QPids, Mandatory, Immediate, Txn, Message),
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------

run_bindings(QPids, IsMandatory, IsImmediate, Txn, Message) ->
    lists:foldl(
      fun (QPid, {Routed, Handled}) ->
              case catch rabbit_amqqueue:deliver(IsMandatory, IsImmediate,
                                                 Txn, Message, QPid) of
                  true             -> {true, [QPid | Handled]};
                  false            -> {true, Handled};
                  {'EXIT', Reason} -> rabbit_log:warning("delivery to ~p failed:~n~p~n",
                                                         [QPid, Reason]),
                                      {Routed, Handled}
              end
      end,
      {false, []},
      QPids).

%% check_delivery(Mandatory, Immediate, {WasRouted, QPids})
check_delivery(true, _   , {false, []}) -> {error, unroutable};
check_delivery(_   , true, {_    , []}) -> {error, not_delivered};
check_delivery(_   , _   , {_    , Qs}) -> {ok, Qs}.
