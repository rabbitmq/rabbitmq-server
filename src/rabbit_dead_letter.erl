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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_dead_letter).

-export([publish/5]).

-include("rabbit.hrl").
-include("rabbit_framing.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type reason() :: 'expired' | 'rejected' | 'maxlen'.

-spec publish(rabbit_types:message(), reason(), rabbit_types:exchange(),
              'undefined' | binary(), rabbit_amqqueue:name()) -> 'ok'.

-endif.

%%----------------------------------------------------------------------------

publish(Msg, Reason, X, RK, QName) ->
    DLMsg = make_msg(Msg, Reason, X#exchange.name, RK, QName),
    Delivery = rabbit_basic:delivery(false, false, DLMsg, undefined),
    {Queues, Cycles} = detect_cycles(Reason, DLMsg,
                                     rabbit_exchange:route(X, Delivery)),
    lists:foreach(fun log_cycle_once/1, Cycles),
    rabbit_amqqueue:deliver(rabbit_amqqueue:lookup(Queues), Delivery),
    ok.

make_msg(Msg = #basic_message{content       = Content,
                              exchange_name = Exchange,
                              routing_keys  = RoutingKeys},
         Reason, DLX, RK, #resource{name = QName}) ->
    {DeathRoutingKeys, HeadersFun1} =
        case RK of
            undefined -> {RoutingKeys, fun (H) -> H end};
            _         -> {[RK], fun (H) -> lists:keydelete(<<"CC">>, 1, H) end}
        end,
    ReasonBin = list_to_binary(atom_to_list(Reason)),
    TimeSec = rabbit_misc:now_ms() div 1000,
    PerMsgTTL = per_msg_ttl_header(Content#content.properties),
    HeadersFun2 =
        fun (Headers) ->
                %% The first routing key is the one specified in the
                %% basic.publish; all others are CC or BCC keys.
                RKs  = [hd(RoutingKeys) | rabbit_basic:header_routes(Headers)],
                RKs1 = [{longstr, Key} || Key <- RKs],
                Info = [{<<"reason">>,       longstr,   ReasonBin},
                        {<<"queue">>,        longstr,   QName},
                        {<<"time">>,         timestamp, TimeSec},
                        {<<"exchange">>,     longstr,   Exchange#resource.name},
                        {<<"routing-keys">>, array,     RKs1}] ++ PerMsgTTL,
                HeadersFun1(rabbit_basic:prepend_table_header(<<"x-death">>,
                                                              Info, Headers))
        end,
    Content1 = #content{properties = Props} =
        rabbit_basic:map_headers(HeadersFun2, Content),
    Content2 = Content1#content{properties =
                                    Props#'P_basic'{expiration = undefined}},
    Msg#basic_message{exchange_name = DLX,
                      id            = rabbit_guid:gen(),
                      routing_keys  = DeathRoutingKeys,
                      content       = Content2}.

per_msg_ttl_header(#'P_basic'{expiration = undefined}) ->
    [];
per_msg_ttl_header(#'P_basic'{expiration = Expiration}) ->
    [{<<"original-expiration">>, longstr, Expiration}];
per_msg_ttl_header(_) ->
    [].

detect_cycles(rejected, _Msg, Queues) ->
    {Queues, []};

detect_cycles(_Reason, #basic_message{content = Content}, Queues) ->
    #content{properties = #'P_basic'{headers = Headers}} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    NoCycles = {Queues, []},
    case Headers of
        undefined ->
            NoCycles;
        _ ->
            case rabbit_misc:table_lookup(Headers, <<"x-death">>) of
                {array, Deaths} ->
                    {Cycling, NotCycling} =
                        lists:partition(fun (#resource{name = Queue}) ->
                                                is_cycle(Queue, Deaths)
                                        end, Queues),
                    OldQueues = [rabbit_misc:table_lookup(D, <<"queue">>) ||
                                    {table, D} <- Deaths],
                    OldQueues1 = [QName || {longstr, QName} <- OldQueues],
                    {NotCycling, [[QName | OldQueues1] ||
                                     #resource{name = QName} <- Cycling]};
                _ ->
                    NoCycles
            end
    end.

is_cycle(Queue, Deaths) ->
    {Cycle, Rest} =
        lists:splitwith(
          fun ({table, D}) ->
                  {longstr, Queue} =/= rabbit_misc:table_lookup(D, <<"queue">>);
              (_) ->
                  true
          end, Deaths),
    %% Is there a cycle, and if so, is it "fully automatic", i.e. with
    %% no reject in it?
    case Rest of
        []    -> false;
        [H|_] -> lists:all(
                   fun ({table, D}) ->
                           {longstr, <<"rejected">>} =/=
                               rabbit_misc:table_lookup(D, <<"reason">>);
                       (_) ->
                           %% There was something we didn't expect, therefore
                           %% a client must have put it there, therefore the
                           %% cycle was not "fully automatic".
                           false
                   end, Cycle ++ [H])
    end.

log_cycle_once(Queues) ->
    Key = {queue_cycle, Queues},
    case get(Key) of
        true      -> ok;
        undefined -> rabbit_log:warning(
                       "Message dropped. Dead-letter queues cycle detected" ++
                       ": ~p~nThis cycle will NOT be reported again.~n",
                       [Queues]),
                     put(Key, true)
    end.
