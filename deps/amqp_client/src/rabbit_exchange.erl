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
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C) 
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit 
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

-export([recover/0, declare/6, lookup/1, lookup_or_die/2,
         list_vhost_exchanges/1,
         simple_publish/6, simple_publish/3,
         route/2, deliver/4]).
-export([add_binding/2, delete_binding/2]).
-export([delete/2]).
-export([maybe_gensym_name/1, check_name/1, check_type/1,
         assert_type/2, topic_matches/2, handlers_isempty/1]).

-export([run_bindings/4]). %% exported for deliver/3's rpc:call usage below ONLY

-import(mnesia).
-import(sets).
-import(lists).
-import(qlc).
-import(regexp).

% FIXME: Restructure binding table to be #binding{exchange, exchange_key_info, handlers}
% and add a secondary index to make lookups quick (?)

recover() ->
    ok = recover_durable_exchanges(),
    ok.

recover_durable_exchanges() ->
    {atomic, ok} = mnesia:transaction
                     (fun () ->
                              mnesia:foldl(fun (Exchange, Acc) ->
                                                   ok = mnesia:write(Exchange),
                                                   Acc
                                           end, ok, durable_exchanges)
                      end),
    ok.

declare(RealmName, NameBin, Type = topic, Durable, AutoDelete, Args) ->
    internal_declare(RealmName, NameBin, Type, Durable, AutoDelete, {sets:new(), Args});
declare(RealmName, NameBin, Type, Durable, AutoDelete, Args) ->
    internal_declare(RealmName, NameBin, Type, Durable, AutoDelete, Args).

internal_declare(RealmName, NameBin, Type, Durable, AutoDelete, Args) ->
    XName = rabbit_misc:r(RealmName, exchange, NameBin),
    Exchange = #exchange{name = XName,
                         type = Type,
                         durable = Durable,
                         auto_delete = AutoDelete,
                         arguments = Args},
    {atomic, ok} = mnesia:transaction
                     (fun () ->
                              ok = mnesia:write(Exchange),
                              if
                                  Durable -> ok = mnesia:write(durable_exchanges, Exchange, write);
                                  true -> ok
                              end
                      end),
    ok = rabbit_realm:add(RealmName, exchange, XName),
    Exchange.

maybe_gensym_name(<<>>) ->
    list_to_binary(rabbit_gensym:gensym("amq.x.gen"));
maybe_gensym_name(NameBin) ->
    NameBin.

check_name(<<"amq.", _/binary>>) ->
    rabbit_misc:die(access_refused, 'exchange.declare');
check_name(_NameBin) ->
    ok.

check_type(<<"fanout">>) ->
    fanout;
check_type(<<"direct">>) ->
    direct;
check_type(<<"topic">>) ->
    topic;
check_type(_) ->
    rabbit_misc:die(command_invalid, 'exchange.declare').

assert_type(#exchange{ type = ActualType }, RequiredType)
  when ActualType == RequiredType ->
    ok;
assert_type(#exchange{ name = XName, type = ActualType }, RequiredType) ->
    rabbit_log:info("Attempt to redeclare exchange ~p from type ~p to type ~p~n",
                    [XName, ActualType, RequiredType]),
    rabbit_misc:die(not_allowed, 'exchange.declare').

lookup(Name) ->
    rabbit_misc:clean_read({exchange, Name}).

lookup_or_die(Name, MethodName) ->
    case lookup(Name) of
        {ok, X} -> X;
        {error, not_found} -> rabbit_misc:die(not_found, MethodName)
    end.

list_vhost_exchanges(VHostPath) ->
    {atomic, Exchanges} =
        mnesia:transaction(
          fun () ->
                  mnesia:match_object(
                    #exchange{name = rabbit_misc:r(VHostPath, exchange),
                              _ = '_'})
          end),
    Exchanges.

handlers_for_missing_binding_record(#exchange{type = Type = topic,
                                              name = ExchangeName},
                                    RoutingKey) ->
    {atomic, Handlers} =
        mnesia:transaction
          (fun () ->
                   BindingKey = delivery_key_for_type(Type, ExchangeName, RoutingKey),
                   case mnesia:wread({binding, BindingKey}) of
                       [#binding{handlers = H}] ->
                           H; % someone's added it concurrently with this attempt
                       [] ->
                           Handlers = compute_topic_routing_key_cache(ExchangeName, RoutingKey),
                           ok = mnesia:write(#binding{key = BindingKey, handlers = Handlers}),
                           Handlers
                   end
           end),
    Handlers;
handlers_for_missing_binding_record(_Exchange, _RoutingKey) ->
    empty_handlers().

empty_handlers() ->
    [].

% Usable by Erlang code that wants to publish messages.
simple_publish(Mandatory, Immediate, ExchangeName, RoutingKeyBin, ContentTypeBin, BodyBin) ->
    {ClassId, _MethodId} = rabbit_framing:method_id('basic.publish'),
    Content = #content{class_id = ClassId,
                       properties = #'P_basic'{content_type = ContentTypeBin},
                       properties_bin = none,
                       payload_fragments_rev = [BodyBin]},
    Message = #basic_message{exchange_name = ExchangeName,
                             routing_key = RoutingKeyBin,
                             content = Content},
    simple_publish(Mandatory, Immediate, Message).

% Usable by Erlang code that wants to publish messages.
simple_publish(Mandatory, Immediate,
               Message = #basic_message{exchange_name = ExchangeName,
                                        routing_key = RoutingKey}) ->
    case lookup(ExchangeName) of
        {ok, Exchange} ->
            Handlers = route(Exchange, RoutingKey),
            deliver(Handlers, Mandatory, Immediate, Message);
        {error, Error} -> {error, Error}
    end.

handlers_isempty([]) -> true;
handlers_isempty(H) when is_list(H) -> false.

route(X = #exchange{name = Name, type = Type, arguments = _Args}, RoutingKey) ->
    BindingKey = delivery_key_for_type(Type, Name, RoutingKey),
    case rabbit_misc:clean_read({binding, BindingKey}) of
        {ok, #binding{handlers = H}} -> H;
        {error, not_found} ->
            handlers_for_missing_binding_record(X, RoutingKey)
    end.

%% check_delivery(Mandatory, Immediate, {RoutedCount, QueueNames})
check_delivery(true, _   , {false, []}) -> {error, unroutable};
check_delivery(_   , true, {_    , []}) -> {error, not_delivered};
check_delivery(_   , _   , {_    , Qs}) -> {ok, Qs}.

deliver([{Node, Hs}], Mandatory, Immediate, Message) when Node == node() ->
    %% optimisation
    check_delivery(Mandatory, Immediate,
                   run_bindings(Hs, Mandatory, Immediate, Message));
deliver(Handlers, Mandatory = false, Immediate = false, Message) ->
    %% optimisation: when Mandatory = false and Immediate = false,
    %% rabbit_amqqueue:deliver in run_bindings below will deliver the
    %% message to the queue process asynchronously, and return true,
    %% which means the handler's associated QName will always be
    %% returned. It is therefore safe to use a fire-and-forget rpc
    %% here and return the QNames - the semantics is preserved. This
    %% scales much better than the non-immediate case below.
    {ok, lists:flatmap(fun ({Node,Hs}) ->
                               rpc:cast(Node, rabbit_exchange, run_bindings,
                                        [Hs, Mandatory, Immediate, Message]),
                               [QName || #handler{queue = QName} <- Hs]
                       end,
                       Handlers)};
deliver(Handlers, Mandatory, Immediate, Message) ->
    %% scatter...
    Keys = lists:map(fun ({Node, Hs}) ->
			     rpc:async_call(Node, rabbit_exchange, run_bindings,
					    [Hs, Mandatory, Immediate, Message])
		     end, Handlers),
    %% ...gather
    {Routed, Handled} =
        lists:foldl(fun (Key, {RoutedAcc, HandledAcc}) ->
                            case rpc:yield(Key) of
                                {badrpc, _Reason} ->
                                    %% TODO: figure out what to log
                                    %% (and do!) here
                                    {RoutedAcc, HandledAcc};
                                {Routed1, Handled1} ->
                                    {Routed1 or RoutedAcc,
                                     %% we do the concatenation below,
                                     %% which should be faster
                                     [Handled1 | HandledAcc]}
                            end
                    end,
                    {false, []},
                    Keys),
    check_delivery(Mandatory, Immediate, {Routed, lists:concat(Handled)}).

run_bindings(Handlers, IsMandatory, IsImmediate, Message) ->
    lists:foldl(
      fun (Handler = #handler{queue = QName, qpid = QPid},
           {Routed, Handled}) ->
              case catch rabbit_amqqueue:deliver(IsMandatory, IsImmediate,
                                                 Message, QPid) of
                  true             -> {true, [QName | Handled]};
                  false            -> {true, Handled};
                  {'EXIT', Reason} -> delivery_failure(none, Handler, Reason),
                                      {Routed, Handled}
              end
      end,
      {false, []},
      Handlers).

delivery_failure(PersistenceMode, Handler, Reason) ->
    rabbit_log:warning("Persistence=~p delivery to ~p failed:~n~p~n", [PersistenceMode, Handler, Reason]),
    delete_binding(Handler).

delivery_key_for_type(fanout, Name, _RoutingKey) ->
    {Name, fanout};
delivery_key_for_type(direct, Name, RoutingKey) ->
    {Name, RoutingKey};
delivery_key_for_type(topic, Name, RoutingKey) ->
    {Name, key, RoutingKey}.

call_with_exchange(Name, Fun) ->
    case mnesia:wread({exchange, Name}) of
        [] -> {error, not_found};
        [X] -> Fun(X)
    end.

atomic_with_exchange(Name, Fun) ->
    {atomic, Result} = mnesia:transaction(fun () -> call_with_exchange(Name, Fun) end),
    Result.

add_binding(BindingSpec = #binding_spec{exchange_name = ExchangeName,
                                        routing_key = RoutingKey,
                                        arguments = _Arguments},
            Q = #amqqueue{}) ->
    call_with_exchange(ExchangeName,
                       fun (X) ->
                               Handler = #handler{binding_spec = BindingSpec,
                                                  queue = Q#amqqueue.name,
                                                  qpid = Q#amqqueue.pid},
                               if
                                   Q#amqqueue.durable and not(X#exchange.durable) ->
                                       mnesia:abort({error, durability_settings_incompatible});
                                   true ->
                                       internal_add_binding(X, RoutingKey, Handler)
                               end
                       end).

delete_binding(BindingSpec, #amqqueue{name = QueueName,
                                      pid = QPid}) ->
    delete_binding(#handler{binding_spec = BindingSpec,
                            queue = QueueName,
                            qpid = QPid}).

delete_binding(Handler = #handler{binding_spec = #binding_spec{exchange_name = ExchangeName,
                                                               routing_key = RoutingKey}}) ->
    atomic_with_exchange(ExchangeName,
                         fun (X) ->
                                 ok = internal_delete_binding(X, RoutingKey, Handler),
                                 maybe_auto_delete(X)
                         end).

% Must run within a transaction.
maybe_auto_delete(#exchange{auto_delete = false}) ->
    ok;
maybe_auto_delete(#exchange{name = ExchangeName, auto_delete = true}) ->
    case internal_delete(ExchangeName, true) of
        {error, in_use} -> ok;
        ok -> ok
    end.

extend_handlers(Handlers, Handler = #handler{qpid = QPid}) ->
    Node = node(QPid),
    case lists:keysearch(Node, 1, Handlers) of
        {value, {Node, Hs}} ->
            lists:keyreplace(Node, 1, Handlers, {Node, [Handler | Hs]});
        false -> [{Node, [Handler]} | Handlers]
    end.

delete_handler(Handlers, Handler = #handler{qpid = QPid}) ->
    Node = node(QPid),
    case lists:keysearch(Node, 1, Handlers) of
        {value, {Node, Hs}} ->
            case lists:delete(Handler, Hs) of
                [] -> lists:keydelete(Node, 1, Handlers);
                Hs1 -> lists:keyreplace(Node, 1, Handlers, {Node, Hs1})
            end;
        false -> Handlers
    end.

% Must run within a transaction.
internal_add_binding(X = #exchange{type = topic, name = ExchangeName, arguments = {PSet, Args}},
                     PatternKey, Handler) ->
    PSet1 = sets:add_element(PatternKey, PSet),
    X1 = X#exchange{arguments = {PSet1, Args}},
    ok = mnesia:write(X1),
    PatternBindingKey = {ExchangeName, pattern, PatternKey},
    ok = add_handler_to_binding(PatternBindingKey, Handler),
    ok = update_topic_routing_key_cache(
           ExchangeName,
           PatternKey,
           fun (B = #binding{handlers = Handlers}) ->
                   ok = mnesia:write(B#binding{handlers =
					       extend_handlers(Handlers, Handler)})
           end);
internal_add_binding(#exchange{name = ExchangeName, type = Type},
                     RoutingKey, Handler) ->
    BindingKey = delivery_key_for_type(Type, ExchangeName, RoutingKey),
    ok = add_handler_to_binding(BindingKey, Handler).

% Must run within a transaction.
internal_delete_binding(X = #exchange{name = ExchangeName,
                                      type = topic,
                                      arguments = {PSet, Args}},
                        PatternKey, Handler) ->
    PatternBindingKey = {ExchangeName, pattern, PatternKey},
    case remove_handler_from_binding(PatternBindingKey, Handler) of
        empty ->
            PSet1 = sets:del_element(PatternKey, PSet),
            X1 = X#exchange{arguments = {PSet1, Args}},
            ok = mnesia:write(X1);
        not_empty ->
            ok
    end,
    ok = update_topic_routing_key_cache(
           ExchangeName,
           PatternKey,
           fun (B = #binding{handlers = H0}) ->
		   H1 = delete_handler(H0, Handler),
		   case handlers_isempty(H1) of
		       true ->
                           ok = mnesia:delete({binding, B#binding.key});
                       _ ->
                           ok = mnesia:write(B#binding{handlers = H1})
                   end
           end);
internal_delete_binding(#exchange{name = ExchangeName, type = Type}, RoutingKey, Handler) ->
    BindingKey = delivery_key_for_type(Type, ExchangeName, RoutingKey),
    remove_handler_from_binding(BindingKey, Handler),
    ok.

% Must run within a transaction.
add_handler_to_binding(BindingKey, Handler) ->
    ok = case mnesia:wread({binding, BindingKey}) of
             [] ->
                 ok = mnesia:write(#binding{key = BindingKey,
					    handlers =
					      extend_handlers(empty_handlers(), Handler)});
             [B = #binding{handlers = H}] ->
                 ok = mnesia:write(B#binding{handlers =
					       extend_handlers(H, Handler)})
         end.

% Must run within a transaction.
remove_handler_from_binding(BindingKey, Handler) ->
    case mnesia:wread({binding, BindingKey}) of
        [] -> empty;
        [B = #binding{handlers = H}] ->
	    H1 = delete_handler(H, Handler),
	    case handlers_isempty(H1) of
		true ->
                    ok = mnesia:delete({binding, BindingKey}),
                    empty;
                _ ->
                    ok = mnesia:write(B#binding{handlers = H1}),
                    not_empty
            end
    end.

% Must run within a transaction.
update_topic_routing_key_cache(ExchangeName, PatternKey, UpdaterFun) ->
    ok = qlc:fold(fun (B, ok) -> UpdaterFun(B) end,
                  ok,
                  qlc:q([B ||
                            B = #binding{key = {BXName, key, RoutingKey}}
                                <- mnesia:table(binding),
                            BXName == ExchangeName,
                            topic_matches(PatternKey, RoutingKey)])).

% Must run within a transaction.
compute_topic_routing_key_cache(ExchangeName, RoutingKey) ->
    dict:to_list(
      qlc:fold(fun (B, Handlers) ->
		       dict:merge(fun (_K, V1, V2) -> V1 ++ V2 end,
				  dict:from_list(B#binding.handlers), Handlers)
	       end,
	       dict:new(),
	       qlc:q([B ||
			 B = #binding{key = {BXName, pattern, PatternKey}}
			     <- mnesia:table(binding),
			 BXName == ExchangeName,
			 topic_matches(PatternKey, RoutingKey)]))).

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
    {atomic, Result} =
        mnesia:transaction(fun () -> internal_delete(ExchangeName, IfUnused) end),
    Result.

bindings_for_exchange(Name) ->
    qlc:e(qlc:q([B || 
                    B = #binding{key = K} <- mnesia:table(binding),
                    element(1, K) == Name])).

internal_delete(ExchangeName, _IfUnused = true) ->
    Bindings = bindings_for_exchange(ExchangeName),
    case Bindings of
        [] -> do_internal_delete(ExchangeName, Bindings);
        _ ->
            case lists:all(fun (#binding{handlers = H}) -> handlers_isempty(H) end,
                           Bindings) of
                true ->
                    %% There are no handlers anywhere in any of the
                    %% bindings for this exchange. NB: This also
                    %% covers the case of topic exchanges, which use
                    %% the binding table to cache patterns.
                    do_internal_delete(ExchangeName, Bindings);
                false ->
                    %% There was at least one real handler
                    %% present. It's still in use.
                    {error, in_use}
            end
    end;
internal_delete(ExchangeName, false) ->
    do_internal_delete(ExchangeName, bindings_for_exchange(ExchangeName)).

forcibly_remove_handlers(H) ->
    lists:foreach(
      fun ({_Node, Hs}) ->
              lists:foreach(
                fun (#handler{binding_spec = BindingSpec,
                              queue = QueueName}) ->
                        ok = rabbit_amqqueue:binding_forcibly_removed(
                               BindingSpec,
                               QueueName)
                end, Hs)
      end, H),
    ok.

do_internal_delete(ExchangeName, Bindings) ->
    case mnesia:wread({exchange, ExchangeName}) of
        [] -> {error, not_found};
        _ ->
            lists:foreach(fun (#binding{key = K, handlers = H}) ->
				  ok = forcibly_remove_handlers(H),
                                  ok = mnesia:delete({binding, K})
                          end, Bindings),
            ok = mnesia:delete({durable_exchanges, ExchangeName}),
            ok = mnesia:delete({exchange, ExchangeName}),
            ok = rabbit_realm:delete_from_all(exchange, ExchangeName)
    end.
