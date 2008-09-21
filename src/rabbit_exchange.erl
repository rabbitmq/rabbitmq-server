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

-module(rabbit_exchange).
-include_lib("stdlib/include/qlc.hrl").
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-export([recover/0, declare/5, lookup/1, lookup_or_die/1,
         list_vhost_exchanges/1,
         simple_publish/6, simple_publish/3,
         route/2]).
-export([add_binding/1, delete_binding/1]).
-export([add_binding/4, delete_binding/4]).
-export([delete/2]).
-export([delete_bindings/1]).
-export([check_type/1, assert_type/2, topic_matches/2]).

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
-spec(list_vhost_exchanges/1 :: (vhost()) -> [exchange()]).
-spec(simple_publish/6 ::
      (bool(), bool(), exchange_name(), routing_key(), binary(), binary()) ->
             publish_res()).
-spec(simple_publish/3 :: (bool(), bool(), message()) -> publish_res()).
-spec(route/2 :: (exchange(), routing_key()) -> [pid()]).
-spec(add_binding/1 :: (binding()) -> 'ok' | not_found() |
                                     {'error', 'durability_settings_incompatible'}).
-spec(delete_binding/1 :: (binding()) -> 'ok' | not_found()).
-spec(add_binding/4 ::
      (queue_name(), exchange_name(), routing_key(), amqp_table()) ->
            bind_res() | {'error', 'durability_settings_incompatible'}).
-spec(delete_binding/4 ::
      (queue_name(), exchange_name(), routing_key(), amqp_table()) ->
             bind_res() | {'error', 'binding_not_found'}).
-spec(delete_bindings/1 :: (queue_name()) -> 'ok' | not_found()).
-spec(topic_matches/2 :: (binary(), binary()) -> bool()).
-spec(delete/2 :: (exchange_name(), bool()) ->
             'ok' | not_found() | {'error', 'in_use'}).

-endif.

%%----------------------------------------------------------------------------

recover() ->
    % TODO: These two functions share commonalities - maybe a refactoring target
    ok = recover_durable_exchanges(),
    ok = recover_durable_routes(),
    ok.

recover_durable_routes() ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              mnesia:foldl(fun (Route, Acc) ->
                                {_, ReverseRoute} = route_with_reverse(Route),
                                ok = mnesia:write(Route),
                                ok = mnesia:write(ReverseRoute),
                                Acc
                            end, ok, durable_routes)
      end).

recover_durable_exchanges() ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              mnesia:foldl(fun (Exchange, Acc) ->
                                   ok = mnesia:write(Exchange),
                                   Acc
                           end, ok, durable_exchanges)
      end).

declare(ExchangeName, Type, Durable, AutoDelete, Args) ->
    Exchange = #exchange{name = ExchangeName,
                         type = Type,
                         durable = Durable,
                         auto_delete = AutoDelete,
                         arguments = Args},
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:wread({exchange, ExchangeName}) of
                  [] -> ok = mnesia:write(Exchange),
                        if Durable ->
                                ok = mnesia:write(
                                       durable_exchanges, Exchange, write);
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
    rabbit_misc:dirty_read({exchange, Name}).

lookup_or_die(Name) ->
    case lookup(Name) of
        {ok, X} -> X;
        {error, not_found} ->
            rabbit_misc:protocol_error(
              not_found, "no ~s", [rabbit_misc:rs(Name)])
    end.

list_vhost_exchanges(VHostPath) ->
    mnesia:dirty_match_object(
      #exchange{name = rabbit_misc:r(VHostPath, exchange), _ = '_'}).

routes_for_exchange(Name) ->
    qlc:e(qlc:q([R || R = #route{binding = #binding{exchange_name = N}} <- mnesia:table(route),
                      N == Name])).

%% Usable by Erlang code that wants to publish messages.
simple_publish(Mandatory, Immediate, ExchangeName, RoutingKeyBin, ContentTypeBin, BodyBin) ->
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
                                        routing_key = RoutingKey}) ->
    case lookup(ExchangeName) of
        {ok, Exchange} ->
            QPids = route(Exchange, RoutingKey),
            rabbit_router:deliver(QPids, Mandatory, Immediate,
                                  none, Message);
        {error, Error} -> {error, Error}
    end.

%% return the list of qpids to which a message with a given routing
%% key, sent to a particular exchange, should be delivered.
%%
%% The function ensures that a qpid appears in the return list exactly
%% as many times as a message should be delivered to it. With the
%% current exchange types that is at most once.
route(#exchange{name = Name, type = topic}, RoutingKey) ->
    route_internal(Name, RoutingKey, fun topic_matches/2);

route(#exchange{name = Name}, RoutingKey) ->
    route_internal(Name, RoutingKey).
    
% TODO: This returns a list of QPids to route to.
% Maybe this should be handled by a cursor instead.
% This routes directly to queues, avoiding any lookup of routes
route_internal(#resource{name = Name, virtual_host = VHostPath}, RoutingKey) ->
    Exchange = #resource{kind = exchange, name ='$1',virtual_host = '$2'},
    MatchHead = #route{binding = #binding{exchange_name = Exchange,
                                          queue_name = '$3',
                                          key = '$4'}},
    Guards = [{'==', '$1', Name}, {'==', '$2', VHostPath}, {'==', '$4', RoutingKey}],
    lookup_qpids(mnesia:activity(async_dirty,
                    fun() -> mnesia:select(route,[{MatchHead, Guards, ['$3']}])
                    end)).
    
% TODO: This returns a list of QPids to route to.
% Maybe this should be handled by a cursor instead.
route_internal(Exchange, RoutingKey, MatchFun) ->
    Query = qlc:q([QName || #route{binding = #binding{exchange_name = ExchangeName,
                                                      queue_name = QName,
                                                      key = BindingKey}} <- mnesia:table(route),
                            ExchangeName == Exchange,
                            % TODO: This causes a full table scan (see bug 19336)
                            MatchFun(BindingKey, RoutingKey)]),
    lookup_qpids(mnesia:activity(async_dirty, fun() -> qlc:e(Query) end)).

lookup_qpids(Queues) ->
    Set = sets:from_list(Queues),
    Fun = fun() ->
            sets:fold(
                fun(Key, Acc) -> [#amqqueue{pid = QPid}] = mnesia:read({amqqueue, Key}),                                 
                                 [QPid] ++ Acc end, 
                [], Set) end,
    mnesia:activity(async_dirty,Fun).
        
% TODO: Should all of the route and binding management not be refactored to it's own module
% Especially seeing as unbind will have to be implemented for 0.91 ?
delete_routes(QueueName) ->
    Binding = #binding{queue_name = QueueName, exchange_name = '_', key = '_'},
    {Route, ReverseRoute} = route_with_reverse(Binding),
    ok = mnesia:delete_object(Route),
    ok = mnesia:delete_object(ReverseRoute),
    ok = mnesia:delete_object(durable_routes, Route, write).

% TODO: Don't really like this double lookup
% It seems very clunky
% Can this get refactored to to avoid the duplication of the lookup/1 function?
call_with_exchange_and_queue(#binding{exchange_name = Exchange,
                                      queue_name = Queue}, Fun) ->
    case mnesia:wread({exchange, Exchange}) of
        [] -> {error, exchange_not_found};
        [X] ->
            case mnesia:wread({amqqueue, Queue}) of
                [] -> {error, queue_not_found};
                [Q] ->
                    Fun(X,Q)
            end
    end.


add_binding(QueueName, ExchangeName, RoutingKey, _Arguments) ->
    Binding = #binding{exchange_name = ExchangeName,
                       key = RoutingKey,
                       queue_name = QueueName},
    rabbit_misc:execute_mnesia_transaction(fun add_binding/1, [Binding]).

delete_binding(QueueName, ExchangeName, RoutingKey, _Arguments) ->
    Binding = #binding{exchange_name = ExchangeName,
                       key = RoutingKey,
                       queue_name = QueueName},
    rabbit_misc:execute_mnesia_transaction(fun delete_binding/1, [Binding]).

% Must be called from within a transaction
add_binding(Binding) ->
    call_with_exchange_and_queue(
        Binding,
        fun (X, Q) -> if Q#amqqueue.durable and not(X#exchange.durable) ->
                            {error, durability_settings_incompatible};
                         true ->
                             ok = sync_binding(Binding, Q#amqqueue.durable, fun mnesia:write/3)
                     end
        end).

% Must be called from within a transaction
delete_binding(Binding) ->
    call_with_exchange_and_queue(
         Binding,
         fun (X, Q) -> ok = sync_binding(Binding, Q#amqqueue.durable, fun mnesia:delete_object/3),
                       maybe_auto_delete(X)
         end).

% TODO: Should the exported function not get renamed to delete_routes instead of this indirection?
delete_bindings(QueueName) ->
    delete_routes(QueueName).

%% Must run within a transaction.
maybe_auto_delete(#exchange{auto_delete = false}) ->
    ok;
maybe_auto_delete(#exchange{name = ExchangeName, auto_delete = true}) ->
    case internal_delete(ExchangeName, true) of
        {error, in_use} -> ok;
        Other -> Other
    end.

reverse_binding(#binding{exchange_name = Exchange, key = Key, queue_name = Queue}) ->
    #reverse_binding{exchange_name = Exchange, key = Key, queue_name = Queue}.

%% Must run within a transaction.
sync_binding(Binding, Durable, Fun) ->
    ok = case Durable of
        true -> Fun(durable_routes, #route{binding = Binding}, write);
        false -> ok
    end,
    [ok, ok] = [Fun(element(1, R), R, write) || R <- tuple_to_list(route_with_reverse(Binding))],
    ok.

route_with_reverse(#route{binding = Binding}) ->
    route_with_reverse(Binding);
route_with_reverse(Binding = #binding{}) ->
    Route = #route{ binding = Binding },
    ReverseRoute = #reverse_route{ reverse_binding = reverse_binding(Binding) },
    {Route, ReverseRoute}.

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

delete(ExchangeName, IfUnused) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () -> internal_delete(ExchangeName, IfUnused) end).

internal_delete(ExchangeName, _IfUnused = true) ->
    Routes = routes_for_exchange(ExchangeName),
    case Routes of
        [] -> do_internal_delete(ExchangeName, Routes);
        _ -> {error, in_use}
    end;
    
internal_delete(ExchangeName, false) ->
    do_internal_delete(ExchangeName, routes_for_exchange(ExchangeName)).

% Don't know if iterating over a list in process memory is cool
% Maybe we should iterate over the DB cursor?
do_internal_delete(ExchangeName, Routes) ->
    case mnesia:wread({exchange, ExchangeName}) of
            [] -> {error, not_found};
            _ ->
                lists:foreach(fun (R) -> ok = mnesia:delete_object(R) end, Routes),
                ok = mnesia:delete({durable_exchanges, ExchangeName}),
                ok = mnesia:delete({exchange, ExchangeName})
    end.
