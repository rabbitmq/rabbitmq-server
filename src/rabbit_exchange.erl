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
         simple_publish/6, simple_publish/3,
         route/3]).
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

-type(publish_res() :: {'ok', [pid()]} |
      not_found() | {'error', 'unroutable' | 'not_delivered'}).
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
%% Retry indefinitely.  This requires that the exchange type hooks
%% should themselves time out, and throw an error on doing so.
-define(MAX_RETRIES, infinity).

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

%% The argument is a thunk that will abort the current attempt,
%% leading mnesia to retry.
retrying_transaction(Func1) ->
    case mnesia:sync_transaction(
           Func1, [(fun () ->
                         exit({aborted, erlang:make_tuple(6, cyclic)})
                    end)], ?MAX_RETRIES) of
        {atomic, Result} -> Result;
        {aborted, nomore} ->
            rabbit_misc:protocol_error(
              internal_error, "exhausted retries for transaction", [])
    end.

declare(ExchangeName, Type, Durable, AutoDelete, Args) ->
    Exchange = #exchange{name = ExchangeName,
                         type = Type,
                         durable = Durable,
                         auto_delete = AutoDelete,
<<<<<<< local
                         arguments = Args},
    ok = Type:declare(Exchange),
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
=======
                         arguments = Args,
                         state = creating},
    case retrying_transaction(
           fun (Retry) ->
                   case mnesia:wread({rabbit_exchange, ExchangeName}) of
                       [] -> ok = mnesia:write(rabbit_exchange, Exchange, write),
                             %io:format("New exchange ~p~n", [Exchange]),
                             {new, Exchange};
                       [ExistingX = #exchange{ state = complete }] ->
                           %io:format("Existing exchange ~p~n", [ExistingX]),
                           {existing, ExistingX};
                       [UncommittedX] ->
                           %io:format("Incomplete exchange ~p~n", [UncommittedX]),
                           Retry()
                   end
           end) of
        {existing, X} -> X;
        {new, X} ->
            NewExchange = X#exchange{ state = complete },
            try
                ok = Type:init(NewExchange)
            catch
                _:Err ->
                    rabbit_misc:execute_mnesia_transaction(
                      fun () ->
                              mnesia:delete(rabbit_exchange, ExchangeName, write) end),
                    throw(Err)
            end,
            rabbit_misc:execute_mnesia_transaction(
              fun () ->
                      %io:format("Completed exchange ~p~n", [NewExchange]),
                      mnesia:write(rabbit_exchange, NewExchange, write),
                      if Durable -> ok = mnesia:write(rabbit_durable_exchange,
                                                      NewExchange, write);
                         true    -> ok
                      end
              end),
            NewExchange
    end.
>>>>>>> other

typename_to_plugin_module(T) ->
    case rabbit_exchange_type:lookup_module(T) of
        {ok, Module} ->
            Module;
        {error, not_found} ->
            rabbit_misc:protocol_error(
              command_invalid, "invalid exchange type '~s'", [T])
    end.

plugin_module_to_typename(M) ->
    {ok, TypeName} = rabbit_exchange_type:lookup_name(M),
    TypeName.

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
    case rabbit_misc:dirty_read({rabbit_exchange, Name}) of
        Res = {ok, #exchange{ state = State }} when State /= creating -> Res;
        _ -> {error, not_found}
    end.

lookup_or_die(Name) ->
    case lookup(Name) of
        {ok, X} -> X;
        {error, not_found} ->
            rabbit_misc:protocol_error(
              not_found, "no ~s", [rabbit_misc:rs(Name)])
    end.

list(VHostPath) ->
    [X || X = #exchange{ state = State } <- 
              mnesia:dirty_match_object(
                rabbit_exchange,
                #exchange{name = rabbit_misc:r(VHostPath, exchange),
                          _ = '_'}),
          State /= creating].

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

<<<<<<< local
publish(X, Seen, Delivery = #delivery{
                   message = #basic_message{routing_key = RK, content = C}}) ->
    case rabbit_router:deliver(route(X, RK, C), Delivery) of
					content = Content}) ->
    case lookup(ExchangeName) of
        {ok, Exchange} ->
            QPids = route(Exchange, RoutingKey, Content),
            rabbit_router:deliver(QPids, Mandatory, Immediate,
                                  none, Message);
        {error, Error} -> {error, Error}
=======
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
>>>>>>> other
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

delete_queue_bindings(QueueName, Retry) ->
    delete_queue_bindings(QueueName, fun delete_forward_routes/1).

delete_transient_queue_bindings(QueueName, Retry) ->
    delete_queue_bindings(QueueName, fun delete_transient_forward_routes/1).

delete_queue_bindings(QueueName, FwdDeleteFun, Retry) ->
    Exchanges = exchanges_for_queue(QueueName),
    [begin
         ok = FwdDeleteFun(reverse_route(Route)),
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

delete_transient_forward_routes(Route) ->
    ok = mnesia:delete_object(rabbit_route, Route, write).

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

%% The following call_with_x procedures will retry until the named
%% exchange has a definite state; i.e., it is 'complete', or doesn't
%% exist.

call_with_exchange(Exchange, Fun) ->
    retrying_transaction(
      fun(Retry) -> case mnesia:read({rabbit_exchange, Exchange}) of
                        [X = #exchange{ state = complete }] ->
                            Fun(X, Retry);
                        []  ->
                            {error, not_found};
                        [_] -> Retry()
                    end
      end).

call_with_exchange_and_queue(Exchange, Queue, Fun) ->
    retrying_transaction(
      fun(Retry) -> case {mnesia:read({rabbit_exchange, Exchange}),
                          mnesia:read({rabbit_queue, Queue})} of
                        {[X = #exchange{ state = complete }], [Q]} ->
                            Fun(X, Q, Retry);
                        {[X = #exchange{ state = complete }], [ ]} ->
                            {error, queue_not_found};
                        {[_] ,  _ } -> Retry();
                        {[ ] , [_]} -> {error, exchange_not_found};
                        {[ ] , [ ]} -> {error, exchange_and_queue_not_found}
                    end
      end).


add_binding(ExchangeName, QueueName, RoutingKey, Arguments) ->
<<<<<<< local
    call_with_exchange_and_queue(
      ExchangeName, QueueName,
      fun (X, Q, B) ->
              if Q#amqqueue.durable and not(X#exchange.durable) ->
                      {error, durability_settings_incompatible};
                 true -> ok = sync_binding(
                                           fun mnesia:write/3)
              end
      end).
=======
    case binding_action(
           ExchangeName, QueueName, RoutingKey, Arguments,
           fun (X, Q, B, Retry) ->
                   case mnesia:read({rabbit_route, B}) of
                       [#route{ state = complete }] -> {existing, X, B};
                       [_] -> Retry();
                       [ ] ->
                           sync_binding(
                             B, Q#amqqueue.durable, creating, fun mnesia:write/3),
                           {new, X, B}
                   end
           end) of
        {existing, X, B} -> B;
        {new, X = #exchange{ type = Type }, B} ->
            Backout = fun() ->
                              rabbit_misc:execute_mnesia_transaction(
                                fun () ->
                                        sync_binding(
                                          B, false, creating, fun mnesia:delete/3)
                                end)
                      end,             
            try
                ok = Type:add_binding(X, B)
            catch
                _:Err ->
                    Backout(),
                    throw(Err)
            end,
            %% FIXME TODO WARNING AWOOGA the exchange or queue may have been created again
            case call_with_exchange_and_queue(
                   ExchangeName, QueueName,
                   fun (X, Q, Retry) ->
                           sync_binding(B, false, complete, fun mnesia:write/3),
                           ok = case X#exchange.durable of
                                    true  -> mnesia:write(rabbit_durable_route,
                                                          #route{binding = Binding}, write);
                                    false -> ok
                                end
                   end) of
                NotFound = {error, _} ->
                    Backout(),
                    NotFound;
                SuccessResult -> SuccessResult
            end
    end.

%% add_binding(ExchangeName, QueueName, RoutingKey, Arguments) ->
%%     binding_action(
%%       ExchangeName, QueueName, RoutingKey, Arguments,
%%       fun (X, Q, B) ->
%%               if Q#amqqueue.durable and not(X#exchange.durable) ->
%%                       {error, durability_settings_incompatible};
%%                  true -> 
                      
%%                       ok = sync_binding(B, Q#amqqueue.durable,
%%                                         fun mnesia:write/3)
%%               end
%%       end).
>>>>>>> other

delete_binding(ExchangeName, QueueName, RoutingKey, Arguments) ->
<<<<<<< local
=======
    binding_action(
      ExchangeName, QueueName, RoutingKey, Arguments,
      fun (X, Q, B, Retry) ->
              case mnesia:match_object(rabbit_route, #route{binding = B},
                                       write) of
                  [] -> {error, binding_not_found};
                  _  -> ok = sync_binding(B, Q#amqqueue.durable, deleting,
                                          fun mnesia:delete_object/3),
                        maybe_auto_delete(X, Retry)
              end
      end).

binding_action(ExchangeName, QueueName, RoutingKey, Arguments, Fun) ->
>>>>>>> other
    call_with_exchange_and_queue(
      ExchangeName, QueueName,
<<<<<<< local
      fun (X, Q) ->
              ok = sync_binding(
                     ExchangeName, QueueName, RoutingKey, Arguments,
      fun (X, Q, B) ->
              maybe_auto_delete(X)
=======
      fun (X, Q, Retry) ->
              Fun(X, Q,
                  #binding{exchange_name = ExchangeName,
                           queue_name    = QueueName,
                           key           = RoutingKey,
                           args          = rabbit_misc:sort_field_table(Arguments)},
                  Retry)
>>>>>>> other
      end).

<<<<<<< local
sync_binding(ExchangeName, QueueName, RoutingKey, Arguments, Durable, Fun) ->
    Binding = #binding{exchange_name = ExchangeName,
                       queue_name = QueueName,
                       key = RoutingKey,
                                 args          = sort_arguments(Arguments)})
    ok = case Durable of
             true  -> Fun(rabbit_durable_route,
                          #route{binding = Binding}, write);
             false -> ok
         end,
=======
% TODO remove durable
sync_binding(Binding, Durable, State, Fun) ->
>>>>>>> other
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
    call_with_exchange(ExchangeName, fun conditional_delete/2);
delete(ExchangeName, _IfUnused = false) ->
    call_with_exchange(ExchangeName, fun unconditional_delete/2).

maybe_auto_delete(#exchange{auto_delete = false}, _) ->
    ok;
maybe_auto_delete(Exchange = #exchange{auto_delete = true}, Retry) ->
    conditional_delete(Exchange, Retry),
    ok.

<<<<<<< local
conditional_delete(Exchange = #exchange{name = ExchangeName}) ->
    case has_bindings(ExchangeName) of
=======
conditional_delete(Exchange = #exchange{name = ExchangeName}, Retry) ->
    Match = #route{binding = #binding{exchange_name = ExchangeName, _ = '_'}},
    %% we need to check for durable routes here too in case a bunch of
    %% routes to durable queues have been removed temporarily as a
    %% result of a node failure
    case contains(rabbit_route, Match) orelse contains(rabbit_durable_route, Match) of
>>>>>>> other
        false  -> unconditional_delete(Exchange);
        true   -> {error, in_use}
    end.

unconditional_delete(#exchange{name = ExchangeName}, Retry) ->
    ok = delete_exchange_bindings(ExchangeName),
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
