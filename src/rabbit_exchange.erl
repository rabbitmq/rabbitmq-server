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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_exchange).
-include_lib("stdlib/include/qlc.hrl").
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-export([recover/0, declare/5, lookup/1, lookup_or_die/1,
         list/1, info/1, info/2, info_all/1, info_all/2,
         simple_publish/6, simple_publish/3,
         route/3]).
-export([add_binding/4, delete_binding/4, list_bindings/1]).
-export([delete/2]).
-export([delete_bindings_for_queue/1]).
-export([check_type/1, assert_type/2, topic_matches/2, headers_match/2]).

%% EXTENDED API
-export([list_exchange_bindings/1]).
-export([list_queue_bindings/1]).

-import(mnesia).
-import(sets).
-import(lists).
-import(qlc).
-import(regexp).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(publish_res() :: {'ok', [pid()]} |
      not_found() | {'error', 'unroutable' | 'not_delivered'}).
-type(bind_res() :: 'ok' |
      {'error', 'queue_not_found' | 'exchange_not_found'}).
-spec(recover/0 :: () -> 'ok').
-spec(declare/5 :: (exchange_name(), exchange_type(), bool(), bool(),
                    amqp_table()) -> exchange()).
-spec(check_type/1 :: (binary()) -> atom()).
-spec(assert_type/2 :: (exchange(), atom()) -> 'ok').
-spec(lookup/1 :: (exchange_name()) -> {'ok', exchange()} | not_found()).
-spec(lookup_or_die/1 :: (exchange_name()) -> exchange()).
-spec(list/1 :: (vhost()) -> [exchange()]).
-spec(info/1 :: (exchange()) -> [info()]).
-spec(info/2 :: (exchange(), [info_key()]) -> [info()]).
-spec(info_all/1 :: (vhost()) -> [[info()]]).
-spec(info_all/2 :: (vhost(), [info_key()]) -> [[info()]]).
-spec(simple_publish/6 ::
      (bool(), bool(), exchange_name(), routing_key(), binary(), binary()) ->
             publish_res()).
-spec(simple_publish/3 :: (bool(), bool(), message()) -> publish_res()).
-spec(route/3 :: (exchange(), routing_key(), decoded_content()) -> [pid()]).
-spec(add_binding/4 ::
      (exchange_name(), queue_name(), routing_key(), amqp_table()) ->
             bind_res() | {'error', 'durability_settings_incompatible'}).
-spec(delete_binding/4 ::
      (exchange_name(), queue_name(), routing_key(), amqp_table()) ->
             bind_res() | {'error', 'binding_not_found'}).
-spec(list_bindings/1 :: (vhost()) -> 
             [{exchange_name(), queue_name(), routing_key(), amqp_table()}]).
-spec(delete_bindings_for_queue/1 :: (queue_name()) -> 'ok').
-spec(topic_matches/2 :: (binary(), binary()) -> bool()).
-spec(headers_match/2 :: (amqp_table(), amqp_table()) -> bool()).
-spec(delete/2 :: (exchange_name(), bool()) ->
             'ok' | not_found() | {'error', 'in_use'}).
-spec(list_queue_bindings/1 :: (queue_name()) -> 
              [{exchange_name(), routing_key(), amqp_table()}]).
-spec(list_exchange_bindings/1 :: (exchange_name()) -> 
              [{queue_name(), routing_key(), amqp_table()}]).

-endif.

%%----------------------------------------------------------------------------

-define(INFO_KEYS, [name, type, durable, auto_delete, arguments].

recover() ->
    ok = rabbit_misc:table_foreach(
           fun(Exchange) -> ok = mnesia:write(rabbit_exchange,
                                              Exchange, write)
           end, rabbit_durable_exchange),
    ok = rabbit_misc:table_foreach(
           fun(Route) -> {_, ReverseRoute} = route_with_reverse(Route),
                         ok = mnesia:write(rabbit_route,
                                           Route, write),
                         ok = mnesia:write(rabbit_reverse_route,
                                           ReverseRoute, write)
           end, rabbit_durable_route).

declare(ExchangeName, Type, Durable, AutoDelete, Args) ->
    Exchange = #exchange{name = ExchangeName,
                         type = Type,
                         durable = Durable,
                         auto_delete = AutoDelete,
                         arguments = Args},
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:wread({rabbit_exchange, ExchangeName}) of
                  [] -> ok = mnesia:write(rabbit_exchange, Exchange, write),
                        if Durable ->
                                ok = mnesia:write(rabbit_durable_exchange,
                                                  Exchange, write);
                           true -> ok
                        end,
                        Exchange;
                  [ExistingX] -> ExistingX
              end
      end).

check_type(<<"fanout">>) ->
    fanout;
check_type(<<"direct">>) ->
    direct;
check_type(<<"topic">>) ->
    topic;
check_type(<<"headers">>) ->
    headers;
check_type(T) ->
    rabbit_misc:protocol_error(
      command_invalid, "invalid exchange type '~s'", [T]).

assert_type(#exchange{ type = ActualType }, RequiredType)
  when ActualType == RequiredType ->
    ok;
assert_type(#exchange{ name = Name, type = ActualType }, RequiredType) ->
    rabbit_misc:protocol_error(
      not_allowed, "cannot redeclare ~s of type '~s' with type '~s'",
      [rabbit_misc:rs(Name), ActualType, RequiredType]).

lookup(Name) ->
    rabbit_misc:dirty_read({rabbit_exchange, Name}).

lookup_or_die(Name) ->
    case lookup(Name) of
        {ok, X} -> X;
        {error, not_found} ->
            rabbit_misc:protocol_error(
              not_found, "no ~s", [rabbit_misc:rs(Name)])
    end.

list(VHostPath) ->
    mnesia:dirty_match_object(
      rabbit_exchange,
      #exchange{name = rabbit_misc:r(VHostPath, exchange), _ = '_'}).

map(VHostPath, F) ->
    %% TODO: there is scope for optimisation here, e.g. using a
    %% cursor, parallelising the function invocation
    lists:map(F, list(VHostPath)).

infos(Items, X) -> [{Item, i(Item, X)} || Item <- Items].

i(name,        #exchange{name        = Name})       -> Name;
i(type,        #exchange{type        = Type})       -> Type;
i(durable,     #exchange{durable     = Durable})    -> Durable;
i(auto_delete, #exchange{auto_delete = AutoDelete}) -> AutoDelete;
i(arguments,   #exchange{arguments   = Arguments})  -> Arguments;
i(Item, _) -> throw({bad_argument, Item}).

info(X = #exchange{}) -> infos(?INFO_KEYS, X).

info(X = #exchange{}, Items) -> infos(Items, X).

info_all(VHostPath) -> map(VHostPath, fun (X) -> info(X) end).

info_all(VHostPath, Items) -> map(VHostPath, fun (X) -> info(X, Items) end).

%% Usable by Erlang code that wants to publish messages.
simple_publish(Mandatory, Immediate, ExchangeName, RoutingKeyBin,
               ContentTypeBin, BodyBin) ->
    {ClassId, _MethodId} = rabbit_framing:method_id('basic.publish'),
    Content = #content{class_id = ClassId,
                       properties = #'P_basic'{content_type = ContentTypeBin},
                       properties_bin = none,
                       payload_fragments_rev = [BodyBin]},
    Message = #basic_message{exchange_name = ExchangeName,
                             routing_key = RoutingKeyBin,
                             content = Content,
                             persistent_key = none},
    simple_publish(Mandatory, Immediate, Message).

%% Usable by Erlang code that wants to publish messages.
simple_publish(Mandatory, Immediate,
               Message = #basic_message{exchange_name = ExchangeName,
                                        routing_key = RoutingKey,
					content = Content}) ->
    case lookup(ExchangeName) of
        {ok, Exchange} ->
            QPids = route(Exchange, RoutingKey, Content),
            rabbit_router:deliver(QPids, Mandatory, Immediate,
                                  none, Message);
        {error, Error} -> {error, Error}
    end.

sort_arguments(Arguments) ->
    lists:keysort(1, Arguments).

%% return the list of qpids to which a message with a given routing
%% key, sent to a particular exchange, should be delivered.
%%
%% The function ensures that a qpid appears in the return list exactly
%% as many times as a message should be delivered to it. With the
%% current exchange types that is at most once.
route(X = #exchange{type = topic}, RoutingKey, _Content) ->
    match_bindings(X, fun (#binding{key = BindingKey}) ->
                              topic_matches(BindingKey, RoutingKey)
                      end);

route(X = #exchange{type = headers}, _RoutingKey, Content) ->
    Headers = case (Content#content.properties)#'P_basic'.headers of
		  undefined -> [];
		  H         -> sort_arguments(H)
	      end,
    match_bindings(X, fun (#binding{args = Spec}) ->
                              headers_match(Spec, Headers)
                      end);

route(X = #exchange{type = fanout}, _RoutingKey, _Content) ->
    match_routing_key(X, '_');

route(X = #exchange{type = direct}, RoutingKey, _Content) ->
    match_routing_key(X, RoutingKey).

%% TODO: Maybe this should be handled by a cursor instead.
%% TODO: This causes a full scan for each entry with the same exchange
match_bindings(#exchange{name = Name}, Match) ->
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

match_routing_key(#exchange{name = Name}, RoutingKey) ->
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

%% TODO: Should all of the route and binding management not be
%% refactored to its own module, especially seeing as unbind will have
%% to be implemented for 0.91 ?

delete_bindings_for_exchange(ExchangeName) ->
    [begin
         ok = mnesia:delete_object(rabbit_reverse_route,
                                   reverse_route(Route), write),
         ok = delete_forward_routes(Route)
     end || Route <- mnesia:match_object(
                       rabbit_route,
                       #route{binding = #binding{exchange_name = ExchangeName,
                                                 _ = '_'}},
                       write)],
    ok.

delete_bindings_for_queue(QueueName) ->
    Exchanges = exchanges_for_queue(QueueName),
    [begin
         ok = delete_forward_routes(reverse_route(Route)),
         ok = mnesia:delete_object(rabbit_reverse_route, Route, write)
     end || Route <- mnesia:match_object(
                       rabbit_reverse_route,
                       reverse_route(
                         #route{binding = #binding{queue_name = QueueName, 
                                                   _ = '_'}}),
                       write)],
    [begin
         [X] = mnesia:read({rabbit_exchange, ExchangeName}),
         ok = maybe_auto_delete(X)
     end || ExchangeName <- Exchanges],
    ok.

delete_forward_routes(Route) ->
    ok = mnesia:delete_object(rabbit_route, Route, write),
    ok = mnesia:delete_object(rabbit_durable_route, Route, write).

exchanges_for_queue(QueueName) ->
    MatchHead = reverse_route(
                  #route{binding = #binding{exchange_name = '$1',
                                            queue_name = QueueName,
                                            _ = '_'}}),
    sets:to_list(
      sets:from_list(
        mnesia:select(rabbit_reverse_route, [{MatchHead, [], ['$1']}]))).

has_bindings(ExchangeName) ->
    MatchHead = #route{binding = #binding{exchange_name = ExchangeName,
                                          _ = '_'}},
    try
        continue(mnesia:select(rabbit_route, [{MatchHead, [], ['$_']}],
                               1, read))
    catch exit:{aborted, {badarg, _}} ->
            %% work around OTP-7025, which was fixed in R12B-1, by
            %% falling back on a less efficient method
            case mnesia:match_object(rabbit_route, MatchHead, read) of
                []    -> false;
                [_|_] -> true
            end
    end.

continue('$end_of_table')    -> false;
continue({[_|_], _})         -> true;
continue({[], Continuation}) -> continue(mnesia:select(Continuation)).

call_with_exchange(Exchange, Fun) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() -> case mnesia:read({rabbit_exchange, Exchange}) of
                   []  -> {error, exchange_not_found};
                   [X] -> Fun(X)
               end
      end).

call_with_exchange_and_queue(Exchange, Queue, Fun) ->
    call_with_exchange(
      Exchange, 
      fun(X) -> case mnesia:read({rabbit_queue, Queue}) of
                    []  -> {error, queue_not_found};
                    [Q] -> Fun(X, Q)
                end
      end).

add_binding(ExchangeName, QueueName, RoutingKey, Arguments) ->
    call_with_exchange_and_queue(
      ExchangeName, QueueName,
      fun (X, Q) ->
              if Q#amqqueue.durable and not(X#exchange.durable) ->
                      {error, durability_settings_incompatible};
                 true -> ok = sync_binding(
                            ExchangeName, QueueName, RoutingKey, Arguments,
                            Q#amqqueue.durable, fun mnesia:write/3)
              end
      end).

delete_binding(ExchangeName, QueueName, RoutingKey, Arguments) ->
    call_with_exchange_and_queue(
      ExchangeName, QueueName,
      fun (X, Q) ->
              ok = sync_binding(
                     ExchangeName, QueueName, RoutingKey, Arguments,
                     Q#amqqueue.durable, fun mnesia:delete_object/3),
              maybe_auto_delete(X)
      end).

sync_binding(ExchangeName, QueueName, RoutingKey, Arguments, Durable, Fun) ->
    Binding = #binding{exchange_name = ExchangeName,
                       queue_name = QueueName,
                       key = RoutingKey,
                       args = sort_arguments(Arguments)},
    ok = case Durable of
             true  -> Fun(rabbit_durable_route,
                          #route{binding = Binding}, write);
             false -> ok
         end,
    {Route, ReverseRoute} = route_with_reverse(Binding),
    ok = Fun(rabbit_route, Route, write),
    ok = Fun(rabbit_reverse_route, ReverseRoute, write),
    ok.

list_bindings(VHostPath) ->
    [{ExchangeName, QueueName, RoutingKey, Arguments} ||
        #route{binding = #binding{
                 exchange_name = ExchangeName,
                 key           = RoutingKey, 
                 queue_name    = QueueName,
                 args          = Arguments}}
            <- mnesia:dirty_match_object(
                 rabbit_route,
                 #route{binding = #binding{
                          exchange_name = rabbit_misc:r(VHostPath, exchange),
                          _ = '_'},
                        _ = '_'})].

route_with_reverse(#route{binding = Binding}) ->
    route_with_reverse(Binding);
route_with_reverse(Binding = #binding{}) ->
    Route = #route{binding = Binding},
    {Route, reverse_route(Route)}.

reverse_route(#route{binding = Binding}) ->
    #reverse_route{reverse_binding = reverse_binding(Binding)};

reverse_route(#reverse_route{reverse_binding = Binding}) ->
    #route{binding = reverse_binding(Binding)}.

reverse_binding(#reverse_binding{exchange_name = Exchange,
                                 queue_name = Queue,
                                 key = Key,
                                 args = Args}) ->
    #binding{exchange_name = Exchange,
             queue_name = Queue,
             key = Key,
             args = Args};

reverse_binding(#binding{exchange_name = Exchange,
                         queue_name = Queue,
                         key = Key,
                         args = Args}) ->
    #reverse_binding{exchange_name = Exchange,
                     queue_name = Queue,
                     key = Key,
                     args = Args}.

default_headers_match_kind() -> all.

parse_x_match(<<"all">>) -> all;
parse_x_match(<<"any">>) -> any;
parse_x_match(Other) ->
    rabbit_log:warning("Invalid x-match field value ~p; expected all or any",
                       [Other]),
    default_headers_match_kind().

%% Horrendous matching algorithm. Depends for its merge-like
%% (linear-time) behaviour on the lists:keysort (sort_arguments) that
%% route/3 and sync_binding/6 do.
%%
%%                 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%% In other words: REQUIRES BOTH PATTERN AND DATA TO BE SORTED ASCENDING BY KEY.
%%                 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%%
headers_match(Pattern, Data) ->
    MatchKind = case lists:keysearch(<<"x-match">>, 1, Pattern) of
		    {value, {_, longstr, MK}} -> parse_x_match(MK);
		    {value, {_, Type, MK}} ->
			rabbit_log:warning("Invalid x-match field type ~p "
                                           "(value ~p); expected longstr",
					   [Type, MK]),
			default_headers_match_kind();
		    _ -> default_headers_match_kind()
		end,
    headers_match(Pattern, Data, true, false, MatchKind).

headers_match([], _Data, AllMatch, _AnyMatch, all) ->
    AllMatch;
headers_match([], _Data, _AllMatch, AnyMatch, any) ->
    AnyMatch;
headers_match([{<<"x-", _/binary>>, _PT, _PV} | PRest], Data,
              AllMatch, AnyMatch, MatchKind) ->
    headers_match(PRest, Data, AllMatch, AnyMatch, MatchKind);
headers_match(_Pattern, [], _AllMatch, AnyMatch, MatchKind) ->
    headers_match([], [], false, AnyMatch, MatchKind);
headers_match(Pattern = [{PK, _PT, _PV} | _], [{DK, _DT, _DV} | DRest],
              AllMatch, AnyMatch, MatchKind) when PK > DK ->
    headers_match(Pattern, DRest, AllMatch, AnyMatch, MatchKind);
headers_match([{PK, _PT, _PV} | PRest], Data = [{DK, _DT, _DV} | _],
              _AllMatch, AnyMatch, MatchKind) when PK < DK ->
    headers_match(PRest, Data, false, AnyMatch, MatchKind);
headers_match([{PK, PT, PV} | PRest], [{DK, DT, DV} | DRest],
              AllMatch, AnyMatch, MatchKind) when PK == DK ->
    {AllMatch1, AnyMatch1} =
        if
            %% It's not properly specified, but a "no value" in a
            %% pattern field is supposed to mean simple presence of
            %% the corresponding data field. I've interpreted that to
            %% mean a type of "void" for the pattern field.
            PT == void -> {AllMatch, true};
	    %% Similarly, it's not specified, but I assume that a
	    %% mismatched type causes a mismatched value.
            PT =/= DT  -> {false, AnyMatch};
            PV == DV   -> {AllMatch, true};
            true       -> {false, AnyMatch}
        end,
    headers_match(PRest, DRest, AllMatch1, AnyMatch1, MatchKind).

split_topic_key(Key) ->
    {ok, KeySplit} = regexp:split(binary_to_list(Key), "\\."),
    KeySplit.

topic_matches(PatternKey, RoutingKey) ->
    P = split_topic_key(PatternKey),
    R = split_topic_key(RoutingKey),
    topic_matches1(P, R).

topic_matches1(["#"], _R) ->
    true;
topic_matches1(["#" | PTail], R) ->
    last_topic_match(PTail, [], lists:reverse(R));
topic_matches1([], []) ->
    true;
topic_matches1(["*" | PatRest], [_ | ValRest]) ->
    topic_matches1(PatRest, ValRest);
topic_matches1([PatElement | PatRest], [ValElement | ValRest]) when PatElement == ValElement ->
    topic_matches1(PatRest, ValRest);
topic_matches1(_, _) ->
    false.

last_topic_match(P, R, []) ->
    topic_matches1(P, R);
last_topic_match(P, R, [BacktrackNext | BacktrackList]) ->
    topic_matches1(P, R) or last_topic_match(P, [BacktrackNext | R], BacktrackList).

delete(ExchangeName, _IfUnused = true) ->
    call_with_exchange(ExchangeName, fun conditional_delete/1);
delete(ExchangeName, _IfUnused = false) ->
    call_with_exchange(ExchangeName, fun unconditional_delete/1).

maybe_auto_delete(#exchange{auto_delete = false}) ->
    ok;
maybe_auto_delete(Exchange = #exchange{auto_delete = true}) ->
    conditional_delete(Exchange),
    ok.

conditional_delete(Exchange = #exchange{name = ExchangeName}) ->
    case has_bindings(ExchangeName) of
        false  -> unconditional_delete(Exchange);
        true   -> {error, in_use}
    end.

unconditional_delete(#exchange{name = ExchangeName}) ->
    ok = delete_bindings_for_exchange(ExchangeName),
    ok = mnesia:delete({rabbit_durable_exchange, ExchangeName}),
    ok = mnesia:delete({rabbit_exchange, ExchangeName}).

%%----------------------------------------------------------------------------
%% EXTENDED API
%% These are API calls that are not used by the server internally,
%% they are exported for embedded clients to use

%% This is currently used in mod_rabbit.erl (XMPP) and expects this to
%% return {QueueName, RoutingKey, Arguments} tuples
list_exchange_bindings(ExchangeName) ->
    Route = #route{binding = #binding{exchange_name = ExchangeName,
                                      _ = '_'}},
    [{QueueName, RoutingKey, Arguments} ||
        #route{binding = #binding{queue_name = QueueName,
                                  key = RoutingKey,
                                  args = Arguments}} 
            <- mnesia:dirty_match_object(rabbit_route, Route)].

% Refactoring is left as an exercise for the reader
list_queue_bindings(QueueName) ->
    Route = #route{binding = #binding{queue_name = QueueName,
                                      _ = '_'}},
    [{ExchangeName, RoutingKey, Arguments} ||
        #route{binding = #binding{exchange_name = ExchangeName,
                                  key = RoutingKey,
                                  args = Arguments}} 
            <- mnesia:dirty_match_object(rabbit_route, Route)].
