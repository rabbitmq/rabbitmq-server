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

-module(rabbit_binding).
-include("rabbit.hrl").

-export([recover/0, add/5, remove/5, list/1]).
-export([list_for_exchange/1, list_for_queue/1]).
%% these must all be run inside a mnesia tx
-export([has_for_exchange/1, remove_for_exchange/1,
         remove_for_queue/1, remove_transient_for_queue/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([key/0]).

-type(key() :: binary()).

-type(bind_res() :: rabbit_types:ok_or_error('queue_not_found' |
                                             'exchange_not_found' |
                                             'exchange_and_queue_not_found')).
-type(inner_fun() ::
        fun((rabbit_types:exchange(), queue()) ->
                   rabbit_types:ok_or_error(rabbit_types:amqp_error()))).

-spec(recover/0 :: () -> [rabbit_types:binding()]).
-spec(add/5 ::
        (rabbit_exchange:name(), rabbit_amqqueue:name(),
         rabbit_router:routing_key(), rabbit_framing:amqp_table(),
         inner_fun()) -> bind_res()).
-spec(remove/5 ::
        (rabbit_exchange:name(), rabbit_amqqueue:name(),
         rabbit_router:routing_key(), rabbit_framing:amqp_table(),
         inner_fun()) -> bind_res() | rabbit_types:error('binding_not_found')).
-spec(list/1 :: (rabbit_types:vhost()) ->
                     [{rabbit_exchange:name(), rabbit_amqqueue:name(),
                       rabbit_router:routing_key(),
                       rabbit_framing:amqp_table()}]).
-spec(list_for_exchange/1 ::
        (rabbit_exchange:name()) -> [{rabbit_amqqueue:name(),
                                      rabbit_router:routing_key(),
                                      rabbit_framing:amqp_table()}]).
-spec(list_for_queue/1 ::
        (rabbit_amqqueue:name()) -> [{rabbit_exchange:name(),
                                      rabbit_router:routing_key(),
                                      rabbit_framing:amqp_table()}]).
-spec(has_for_exchange/1 :: (rabbit_exchange:name()) -> boolean()).
-spec(remove_for_exchange/1 ::
        (rabbit_exchange:name()) -> [rabbit_types:binding()]).
-spec(remove_for_queue/1 ::
        (rabbit_amqqueue:name()) -> fun (() -> any())).
-spec(remove_transient_for_queue/1 ::
        (rabbit_amqqueue:name()) -> fun (() -> any())).

-endif.

%%----------------------------------------------------------------------------

recover() ->
    rabbit_misc:table_fold(
      fun (Route = #route{binding = B}, Acc) ->
              {_, ReverseRoute} = route_with_reverse(Route),
              ok = mnesia:write(rabbit_route, Route, write),
              ok = mnesia:write(rabbit_reverse_route, ReverseRoute, write),
              [B | Acc]
      end, [], rabbit_durable_route).

add(ExchangeName, QueueName, RoutingKey, Arguments, InnerFun) ->
    case binding_action(
           ExchangeName, QueueName, RoutingKey, Arguments,
           fun (X, Q, B) ->
                   %% this argument is used to check queue exclusivity;
                   %% in general, we want to fail on that in preference to
                   %% anything else
                   case InnerFun(X, Q) of
                       ok ->
                           case mnesia:read({rabbit_route, B}) of
                               [] ->
                                   ok = sync_binding(B,
                                                     X#exchange.durable andalso
                                                     Q#amqqueue.durable,
                                                     fun mnesia:write/3),
                                   rabbit_event:notify(
                                     binding_created,
                                     [{exchange_name, ExchangeName},
                                      {queue_name, QueueName},
                                      {routing_key, RoutingKey},
                                      {arguments, Arguments}]),
                                   {new, X, B};
                               [_R] ->
                                   {existing, X, B}
                           end;
                       {error, _} = E ->
                           E
                   end
           end) of
        {new, Exchange = #exchange{ type = Type }, Binding} ->
            (type_to_module(Type)):add_binding(Exchange, Binding);
        {existing, _, _} ->
            ok;
        {error, _} = Err ->
            Err
    end.

remove(ExchangeName, QueueName, RoutingKey, Arguments, InnerFun) ->
    case binding_action(
           ExchangeName, QueueName, RoutingKey, Arguments,
           fun (X, Q, B) ->
                   case mnesia:match_object(rabbit_route, #route{binding = B},
                                            write) of
                       [] ->
                           {error, binding_not_found};
                       _  ->
                           case InnerFun(X, Q) of
                               ok ->
                                   ok =
                                       sync_binding(B,
                                                    X#exchange.durable andalso
                                                    Q#amqqueue.durable,
                                                    fun mnesia:delete_object/3),
                                   rabbit_event:notify(
                                     binding_deleted,
                                     [{exchange_name, ExchangeName},
                                      {queue_name, QueueName}]),
                                   Del = rabbit_exchange:maybe_auto_delete(X),
                                   {{Del, X}, B};
                               {error, _} = E ->
                                   E
                           end
                   end
           end) of
        {error, _} = Err ->
            Err;
        {{IsDeleted, X = #exchange{ type = Type }}, B} ->
            Module = type_to_module(Type),
            case IsDeleted of
                auto_deleted -> Module:delete(X, [B]);
                not_deleted  -> Module:remove_bindings(X, [B])
            end
    end.

list(VHostPath) ->
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
                          _             = '_'},
                        _       = '_'})].

list_for_exchange(ExchangeName) ->
    Route = #route{binding = #binding{exchange_name = ExchangeName, _ = '_'}},
    [{QueueName, RoutingKey, Arguments} ||
        #route{binding = #binding{queue_name = QueueName,
                                  key        = RoutingKey,
                                  args       = Arguments}}
            <- mnesia:dirty_match_object(rabbit_route, Route)].

% Refactoring is left as an exercise for the reader
list_for_queue(QueueName) ->
    Route = #route{binding = #binding{queue_name = QueueName, _ = '_'}},
    [{ExchangeName, RoutingKey, Arguments} ||
        #route{binding = #binding{exchange_name = ExchangeName,
                                  key           = RoutingKey,
                                  args          = Arguments}}
            <- mnesia:dirty_match_object(rabbit_route, Route)].

has_for_exchange(ExchangeName) ->
    Match = #route{binding = #binding{exchange_name = ExchangeName, _ = '_'}},
    %% we need to check for durable routes here too in case a bunch of
    %% routes to durable queues have been removed temporarily as a
    %% result of a node failure
    contains(rabbit_route, Match) orelse contains(rabbit_durable_route, Match).

remove_for_exchange(ExchangeName) ->
    [begin
         ok = mnesia:delete_object(rabbit_reverse_route,
                                   reverse_route(Route), write),
         ok = delete_forward_routes(Route),
         Route#route.binding
     end || Route <- mnesia:match_object(
                       rabbit_route,
                       #route{binding = #binding{exchange_name = ExchangeName,
                                                 _ = '_'}},
                       write)].

remove_for_queue(QueueName) ->
    remove_for_queue(QueueName, fun delete_forward_routes/1).

remove_transient_for_queue(QueueName) ->
    remove_for_queue(QueueName, fun delete_transient_forward_routes/1).

%%----------------------------------------------------------------------------

binding_action(ExchangeName, QueueName, RoutingKey, Arguments, Fun) ->
    call_with_exchange_and_queue(
      ExchangeName, QueueName,
      fun (X, Q) ->
              Fun(X, Q, #binding{
                       exchange_name = ExchangeName,
                       queue_name    = QueueName,
                       key           = RoutingKey,
                       args          = rabbit_misc:sort_field_table(Arguments)})
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

call_with_exchange_and_queue(Exchange, Queue, Fun) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () -> case {mnesia:read({rabbit_exchange, Exchange}),
                      mnesia:read({rabbit_queue, Queue})} of
                   {[X], [Q]} -> Fun(X, Q);
                   {[ ], [_]} -> {error, exchange_not_found};
                   {[_], [ ]} -> {error, queue_not_found};
                   {[ ], [ ]} -> {error, exchange_and_queue_not_found}
               end
      end).

%% Used with atoms from records; e.g., the type is expected to exist.
type_to_module(T) ->
    {ok, Module} = rabbit_exchange_type_registry:lookup_module(T),
    Module.

contains(Table, MatchHead) ->
    continue(mnesia:select(Table, [{MatchHead, [], ['$_']}], 1, read)).

continue('$end_of_table')    -> false;
continue({[_|_], _})         -> true;
continue({[], Continuation}) -> continue(mnesia:select(Continuation)).

remove_for_queue(QueueName, FwdDeleteFun) ->
    DeletedBindings =
        [begin
             Route = reverse_route(ReverseRoute),
             ok = FwdDeleteFun(Route),
             ok = mnesia:delete_object(rabbit_reverse_route,
                                       ReverseRoute, write),
             Route#route.binding
         end || ReverseRoute
                    <- mnesia:match_object(
                         rabbit_reverse_route,
                         reverse_route(#route{binding = #binding{
                                                queue_name = QueueName,
                                                _          = '_'}}),
                         write)],
    Cleanup = cleanup_removed_queue_bindings(
                lists:keysort(#binding.exchange_name, DeletedBindings), []),
    fun () ->
            lists:foreach(
              fun ({{IsDeleted, X = #exchange{ type = Type }}, Bs}) ->
                      Module = type_to_module(Type),
                      case IsDeleted of
                          auto_deleted -> Module:delete(X, Bs);
                          not_deleted  -> Module:remove_bindings(X, Bs)
                      end
              end, Cleanup)
    end.

%% Requires that its input binding list is sorted in exchange-name
%% order, so that the grouping of bindings (for passing to
%% cleanup_removed_queue_bindings1) works properly.
cleanup_removed_queue_bindings([], Acc) ->
    Acc;
cleanup_removed_queue_bindings(
  [B = #binding{exchange_name = ExchangeName} | Bs], Acc) ->
    cleanup_removed_queue_bindings(ExchangeName, Bs, [B], Acc).

cleanup_removed_queue_bindings(
  ExchangeName, [B = #binding{exchange_name = ExchangeName} | Bs],
  Bindings, Acc) ->
    cleanup_removed_queue_bindings(ExchangeName, Bs, [B | Bindings], Acc);
cleanup_removed_queue_bindings(ExchangeName, Removed, Bindings, Acc) ->
    %% either Removed is [], or its head has a non-matching ExchangeName
    NewAcc = [cleanup_removed_queue_bindings1(ExchangeName, Bindings) | Acc],
    cleanup_removed_queue_bindings(Removed, NewAcc).

cleanup_removed_queue_bindings1(ExchangeName, Bindings) ->
    [X] = mnesia:read({rabbit_exchange, ExchangeName}),
    {{rabbit_exchange:maybe_auto_delete(X), X}, Bindings}.

delete_forward_routes(Route) ->
    ok = mnesia:delete_object(rabbit_route, Route, write),
    ok = mnesia:delete_object(rabbit_durable_route, Route, write).

delete_transient_forward_routes(Route) ->
    ok = mnesia:delete_object(rabbit_route, Route, write).

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
                                 queue_name    = Queue,
                                 key           = Key,
                                 args          = Args}) ->
    #binding{exchange_name = Exchange,
             queue_name    = Queue,
             key           = Key,
             args          = Args};

reverse_binding(#binding{exchange_name = Exchange,
                         queue_name    = Queue,
                         key           = Key,
                         args          = Args}) ->
    #reverse_binding{exchange_name = Exchange,
                     queue_name    = Queue,
                     key           = Key,
                     args          = Args}.
