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
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-export([recover/0, declare/5, lookup/1, lookup_or_die/1,
         list/1, info/1, info/2, info_all/1, info_all/2,
         publish/2]).
-export([add_binding/4, delete_binding/4, list_bindings/1]).
-export([delete/2]).
-export([delete_queue_bindings/1, delete_transient_queue_bindings/1]).
-export([check_type/1, assert_type/2]).

%% EXTENDED API
-export([list_exchange_bindings/1]).
-export([list_queue_bindings/1]).

-import(mnesia).
-import(sets).
-import(lists).
-import(regexp).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(bind_res() :: 'ok' | {'error',
                            'queue_not_found' |
                            'exchange_not_found' |
                            'exchange_and_queue_not_found'}).
-spec(recover/0 :: () -> 'ok').
-spec(declare/5 :: (exchange_name(), exchange_type(), boolean(), boolean(),
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
-spec(publish/2 :: (exchange(), delivery()) -> {routing_result(), [pid()]}).
-spec(add_binding/4 ::
      (exchange_name(), queue_name(), routing_key(), amqp_table()) ->
             bind_res() | {'error', 'durability_settings_incompatible'}).
-spec(delete_binding/4 ::
      (exchange_name(), queue_name(), routing_key(), amqp_table()) ->
             bind_res() | {'error', 'binding_not_found'}).
-spec(list_bindings/1 :: (vhost()) -> 
             [{exchange_name(), queue_name(), routing_key(), amqp_table()}]).
-spec(delete_queue_bindings/1 :: (queue_name()) -> 'ok').
-spec(delete_transient_queue_bindings/1 :: (queue_name()) -> 'ok').
-spec(delete/2 :: (exchange_name(), boolean()) ->
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
           end, rabbit_durable_route),
    %% Tell exchanges to recover themselves only *after* we've
    %% recovered their bindings.
    ok = rabbit_misc:table_foreach(
           fun(Exchange = #exchange{type = Type}) ->
                   ok = Type:recover(Exchange)
           end, rabbit_durable_exchange).

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
                        ok = Type:init(Exchange),
                        Exchange;
                  [ExistingX] -> ExistingX
              end
      end).

typename_to_plugin_module(T) when is_binary(T) ->
    case catch list_to_existing_atom("rabbit_exchange_type_" ++ binary_to_list(T)) of
        {'EXIT', {badarg, _}} ->
            rabbit_misc:protocol_error(
              command_invalid, "invalid exchange type '~s'", [T]);
        Module ->
            Module
    end.

plugin_module_to_typename(M) when is_atom(M) ->
    "rabbit_exchange_type_" ++ S = atom_to_list(M),
    list_to_binary(S).

check_type(T) ->
    Module = typename_to_plugin_module(T),
    case catch Module:description() of
        {'EXIT', {undef, [{_, description, []} | _]}} ->
            rabbit_misc:protocol_error(
              command_invalid, "invalid exchange type '~s'", [T]);
        {'EXIT', _} ->
            rabbit_misc:protocol_error(
              command_invalid, "problem loading exchange type '~s'", [T]);
        _ ->
            Module
    end.

assert_type(#exchange{ type = ActualType }, RequiredType)
  when ActualType == RequiredType ->
    ok;
assert_type(#exchange{ name = Name, type = ActualType }, RequiredType) ->
    rabbit_misc:protocol_error(
      not_allowed, "cannot redeclare ~s of type '~s' with type '~s'",
      [rabbit_misc:rs(Name),
       plugin_module_to_typename(ActualType),
       plugin_module_to_typename(RequiredType)]).

lookup(Name) ->
    rabbit_misc:dirty_read({rabbit_exchange, Name}).

lookup_or_die(Name) ->
    case lookup(Name) of
        {ok, X} -> X;
        {error, not_found} -> rabbit_misc:not_found(Name)
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
i(type,        #exchange{type        = Type})       -> plugin_module_to_typename(Type);
i(durable,     #exchange{durable     = Durable})    -> Durable;
i(auto_delete, #exchange{auto_delete = AutoDelete}) -> AutoDelete;
i(arguments,   #exchange{arguments   = Arguments})  -> Arguments;
i(Item, _) -> throw({bad_argument, Item}).

info(X = #exchange{}) -> infos(?INFO_KEYS, X).

info(X = #exchange{}, Items) -> infos(Items, X).

info_all(VHostPath) -> map(VHostPath, fun (X) -> info(X) end).

info_all(VHostPath, Items) -> map(VHostPath, fun (X) -> info(X, Items) end).

publish(X, Delivery) ->
    publish(X, [], Delivery).

publish(X = #exchange{type = Type}, Seen, Delivery) ->
    case Type:publish(X, Delivery) of
        {_, []} = R ->
            #exchange{name = XName, arguments = Args} = X,
            case rabbit_misc:r_arg(XName, exchange, Args,
                                   <<"alternate-exchange">>) of
                undefined ->
                    R;
                AName ->
                    NewSeen = [XName | Seen],
                    case lists:member(AName, NewSeen) of
                        true ->
                            R;
                        false ->
                            case lookup(AName) of
                                {ok, AX} ->
                                    publish(AX, NewSeen, Delivery);
                                {error, not_found} ->
                                    rabbit_log:warning(
                                      "alternate exchange for ~s "
                                      "does not exist: ~s",
                                      [rabbit_misc:rs(XName),
                                       rabbit_misc:rs(AName)]),
                                    R
                            end
                    end
            end;
        R ->
            R
    end.

%% TODO: Should all of the route and binding management not be
%% refactored to its own module, especially seeing as unbind will have
%% to be implemented for 0.91 ?

delete_exchange_bindings(ExchangeName) ->
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

delete_queue_bindings(QueueName) ->
    delete_queue_bindings(QueueName, fun delete_forward_routes/1).

delete_transient_queue_bindings(QueueName) ->
    delete_queue_bindings(QueueName, fun delete_transient_forward_routes/1).

delete_queue_bindings(QueueName, FwdDeleteFun) ->
    DeletedBindings =
        [begin
             FwdRoute = reverse_route(Route),
             ok = FwdDeleteFun(FwdRoute),
             ok = mnesia:delete_object(rabbit_reverse_route, Route, write),
             FwdRoute#route.binding
         end || Route <- mnesia:match_object(
                           rabbit_reverse_route,
                           reverse_route(
                             #route{binding = #binding{queue_name = QueueName, 
                                                       _ = '_'}}),
                           write)],
    %% We need the keysort to group the bindings by exchange name, so
    %% that cleanup_deleted_queue_bindings can inform the exchange of
    %% its vanished bindings before maybe_auto_delete'ing the
    %% exchange.
    ok = cleanup_deleted_queue_bindings(lists:keysort(#binding.exchange_name, DeletedBindings),
                                        none, []).

%% Requires that its input binding list is sorted in exchange-name
%% order, so that the grouping of bindings (for passing to
%% cleanup_deleted_queue_bindings1) works properly.
cleanup_deleted_queue_bindings([], ExchangeName, Bindings) ->
    cleanup_deleted_queue_bindings1(ExchangeName, Bindings);
cleanup_deleted_queue_bindings([B = #binding{exchange_name = N} | Rest], ExchangeName, Bindings)
  when N =:= ExchangeName ->
    cleanup_deleted_queue_bindings(Rest, ExchangeName, [B | Bindings]);
cleanup_deleted_queue_bindings([B = #binding{exchange_name = N} | Rest], ExchangeName, Bindings) ->
    cleanup_deleted_queue_bindings1(ExchangeName, Bindings),
    cleanup_deleted_queue_bindings(Rest, N, [B]).

cleanup_deleted_queue_bindings1(none, []) ->
    ok;
cleanup_deleted_queue_bindings1(ExchangeName, Bindings) ->
    [X = #exchange{type = Type}] = mnesia:read({rabbit_exchange, ExchangeName}),
    [ok = Type:delete_binding(X, B) || B <- Bindings],
    ok = maybe_auto_delete(X).

delete_forward_routes(Route) ->
    ok = mnesia:delete_object(rabbit_route, Route, write),
    ok = mnesia:delete_object(rabbit_durable_route, Route, write).

delete_transient_forward_routes(Route) ->
    ok = mnesia:delete_object(rabbit_route, Route, write).

contains(Table, MatchHead) ->
    try
        continue(mnesia:select(Table, [{MatchHead, [], ['$_']}], 1, read))
    catch exit:{aborted, {badarg, _}} ->
            %% work around OTP-7025, which was fixed in R12B-1, by
            %% falling back on a less efficient method
            case mnesia:match_object(Table, MatchHead, read) of
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
                   []  -> {error, not_found};
                   [X] -> Fun(X)
               end
      end).

call_with_exchange_and_queue(Exchange, Queue, Fun) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() -> case {mnesia:read({rabbit_exchange, Exchange}),
                     mnesia:read({rabbit_queue, Queue})} of
                   {[X], [Q]} -> Fun(X, Q);
                   {[ ], [_]} -> {error, exchange_not_found};
                   {[_], [ ]} -> {error, queue_not_found};
                   {[ ], [ ]} -> {error, exchange_and_queue_not_found}
               end
      end).

add_binding(ExchangeName, QueueName, RoutingKey, Arguments) ->
    binding_action(
      ExchangeName, QueueName, RoutingKey, Arguments,
      fun (X = #exchange{type = Type}, Q, B) ->
              if Q#amqqueue.durable and not(X#exchange.durable) ->
                      {error, durability_settings_incompatible};
                 true -> ok = sync_binding(B, Q#amqqueue.durable,
                                           fun mnesia:write/3),
                         ok = Type:add_binding(X, B)
              end
      end).

delete_binding(ExchangeName, QueueName, RoutingKey, Arguments) ->
    binding_action(
      ExchangeName, QueueName, RoutingKey, Arguments,
      fun (X = #exchange{type = Type}, Q, B) ->
              case mnesia:match_object(rabbit_route, #route{binding = B},
                                       write) of
                  [] -> {error, binding_not_found};
                  _  -> ok = sync_binding(B, Q#amqqueue.durable,
                                          fun mnesia:delete_object/3),
                        ok = Type:delete_binding(X, B),
                        maybe_auto_delete(X)
              end
      end).

binding_action(ExchangeName, QueueName, RoutingKey, Arguments, Fun) ->
    call_with_exchange_and_queue(
      ExchangeName, QueueName,
      fun (X, Q) ->
              Fun(X, Q, #binding{exchange_name = ExchangeName,
                                 queue_name    = QueueName,
                                 key           = RoutingKey,
                                 args          = rabbit_misc:sort_arguments(Arguments)})
      end).

sync_binding(Binding, Durable, Fun) ->
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
    Match = #route{binding = #binding{exchange_name = ExchangeName, _ = '_'}},
    %% we need to check for durable routes here too in case a bunch of
    %% routes to durable queues have been removed temporarily as a
    %% result of a node failure
    case contains(rabbit_route, Match) orelse contains(rabbit_durable_route, Match) of
        false  -> unconditional_delete(Exchange);
        true   -> {error, in_use}
    end.

unconditional_delete(X = #exchange{name = ExchangeName, type = Type}) ->
    ok = delete_exchange_bindings(ExchangeName),
    ok = mnesia:delete({rabbit_durable_exchange, ExchangeName}),
    ok = mnesia:delete({rabbit_exchange, ExchangeName}),
    ok = Type:delete(X).

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
