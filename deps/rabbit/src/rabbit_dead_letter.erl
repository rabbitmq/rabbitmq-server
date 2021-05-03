%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_dead_letter).

-export([publish/5]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

%%----------------------------------------------------------------------------

-type reason() :: 'expired' | 'rejected' | 'maxlen' | delivery_limit.

%%----------------------------------------------------------------------------

-spec publish(rabbit_types:message(), reason(), rabbit_types:exchange(),
              'undefined' | binary(), rabbit_amqqueue:name()) -> 'ok'.

publish(Msg, Reason, X, RK, QName) ->
    DLMsg = make_msg(Msg, Reason, X#exchange.name, RK, QName),
    Delivery = rabbit_basic:delivery(false, false, DLMsg, undefined),
    {Queues, Cycles} = detect_cycles(Reason, DLMsg,
                                     rabbit_exchange:route(X, Delivery)),
    lists:foreach(fun log_cycle_once/1, Cycles),
    rabbit_amqqueue:deliver(rabbit_amqqueue:lookup(Queues), Delivery).

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
    TimeSec = os:system_time(seconds),
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
                HeadersFun1(update_x_death_header(Info, Headers))
        end,
    Content1 = #content{properties = Props} =
        rabbit_basic:map_headers(HeadersFun2, Content),
    Content2 = Content1#content{properties =
                                    Props#'P_basic'{expiration = undefined}},
    Msg#basic_message{exchange_name = DLX,
                      id            = rabbit_guid:gen(),
                      routing_keys  = DeathRoutingKeys,
                      content       = Content2}.


x_death_event_key(Info, Key) ->
    case lists:keysearch(Key, 1, Info) of
        false                         -> undefined;
        {value, {Key, _KeyType, Val}} -> Val
    end.

maybe_append_to_event_group(Table, _Key, _SeenKeys, []) ->
    [Table];
maybe_append_to_event_group(Table, {_Queue, _Reason} = Key, SeenKeys, Acc) ->
    case sets:is_element(Key, SeenKeys) of
        true  -> Acc;
        false -> [Table | Acc]
    end.

group_by_queue_and_reason([]) ->
    [];
group_by_queue_and_reason([Table]) ->
    [Table];
group_by_queue_and_reason(Tables) ->
    {_, Grouped} =
        lists:foldl(
          fun ({table, Info}, {SeenKeys, Acc}) ->
                  Q = x_death_event_key(Info, <<"queue">>),
                  R = x_death_event_key(Info, <<"reason">>),
                  Matcher = queue_and_reason_matcher(Q, R),
                  {Matches, _} = lists:partition(Matcher, Tables),
                  {Augmented, N} = case Matches of
                                       [X]        -> {X, 1};
                                       [X|_] = Xs -> {X, length(Xs)}
                                   end,
                  Key = {Q, R},
                  Acc1 = maybe_append_to_event_group(
                           ensure_xdeath_event_count(Augmented, N),
                           Key, SeenKeys, Acc),
                  {sets:add_element(Key, SeenKeys), Acc1}
          end, {sets:new(), []}, Tables),
    Grouped.

update_x_death_header(Info, undefined) ->
    update_x_death_header(Info, []);
update_x_death_header(Info, Headers) ->
    X = x_death_event_key(Info, <<"exchange">>),
    Q = x_death_event_key(Info, <<"queue">>),
    R = x_death_event_key(Info, <<"reason">>),
    case rabbit_basic:header(<<"x-death">>, Headers) of
        undefined ->
            %% First x-death event gets its own top-level headers.
            %% See rabbitmq/rabbitmq-server#1332.
            Headers2 = rabbit_misc:set_table_value(Headers, <<"x-first-death-reason">>,
                                                   longstr, R),
            Headers3 = rabbit_misc:set_table_value(Headers2, <<"x-first-death-queue">>,
                                                   longstr, Q),
            Headers4 = rabbit_misc:set_table_value(Headers3, <<"x-first-death-exchange">>,
                                                   longstr, X),
            rabbit_basic:prepend_table_header(
              <<"x-death">>,
              [{<<"count">>, long, 1} | Info], Headers4);
        {<<"x-death">>, array, Tables} ->
            %% group existing x-death headers in case we have some from
            %% before rabbitmq-server#78
            GroupedTables = group_by_queue_and_reason(Tables),
            {Matches, Others} = lists:partition(
                                  queue_and_reason_matcher(Q, R),
                                  GroupedTables),
            Info1 = case Matches of
                        [] ->
                            [{<<"count">>, long, 1} | Info];
                        [{table, M}] ->
                            increment_xdeath_event_count(M)
                    end,
            rabbit_misc:set_table_value(
              Headers, <<"x-death">>, array,
              [{table, rabbit_misc:sort_field_table(Info1)} | Others]);
        {<<"x-death">>, InvalidType, Header} ->
            _ = rabbit_log:warning("Message has invalid x-death header (type: ~p)."
                               " Resetting header ~p~n",
                               [InvalidType, Header]),
            %% if x-death is something other than an array (list)
            %% then we reset it: this happens when some clients consume
            %% a message and re-publish is, converting header values
            %% to strings, intentionally or not.
            %% See rabbitmq/rabbitmq-server#767 for details.
            rabbit_misc:set_table_value(
              Headers, <<"x-death">>, array,
              [{table, [{<<"count">>, long, 1} | Info]}])
    end.

ensure_xdeath_event_count({table, Info}, InitialVal) when InitialVal >= 1 ->
    {table, ensure_xdeath_event_count(Info, InitialVal)};
ensure_xdeath_event_count(Info, InitialVal) when InitialVal >= 1 ->
    case x_death_event_key(Info, <<"count">>) of
        undefined ->
            [{<<"count">>, long, InitialVal} | Info];
        _ ->
            Info
    end.

increment_xdeath_event_count(Info) ->
    case x_death_event_key(Info, <<"count">>) of
        undefined ->
            [{<<"count">>, long, 1} | Info];
        N ->
            lists:keyreplace(
              <<"count">>, 1, Info,
              {<<"count">>, long, N + 1})
    end.

queue_and_reason_matcher(Q, R) ->
    F = fun(Info) ->
                x_death_event_key(Info, <<"queue">>) =:= Q
                    andalso x_death_event_key(Info, <<"reason">>) =:= R
        end,
    fun({table, Info}) ->
            F(Info);
       (Info) when is_list(Info) ->
            F(Info)
    end.

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
        undefined -> _ = rabbit_log:warning(
                       "Message dropped. Dead-letter queues cycle detected" ++
                           ": ~p~nThis cycle will NOT be reported again.~n",
                       [Queues]),
                     put(Key, true)
    end.
