%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_queue_type).
-feature(maybe_expr, enable).

-behaviour(rabbit_registry_class).

-include("amqqueue.hrl").
-include("vhost.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("amqp10_common/include/amqp10_types.hrl").

-export([
         init/0,
         close/1,
         discover/1,
         short_alias_of/1,
         default/0,
         default_alias/0,
         fallback/0,
         inject_dqt/1,
         vhosts_with_dqt/1,
         is_enabled/1,
         is_compatible/4,
         declare/2,
         delete/4,
         is_recoverable/1,
         recover/2,
         purge/1,
         policy_changed/1,
         stat/1,
         format/2,
         remove/2,
         info/2,
         state_info/1,
         format_status/1,
         info_down/2,
         info_down/3,
         %% stateful client API
         new/2,
         consume/3,
         cancel/3,
         handle_down/4,
         handle_event/3,
         module/2,
         deliver/4,
         settle/5,
         credit_v1/5,
         credit/6,
         dequeue/5,
         fold_state/3,
         is_policy_applicable/2,
         is_server_named_allowed/1,
         amqp_capabilities/1,
         arguments/1,
         arguments/2,
         notify_decorators/1,
         publish_at_most_once/2,
         can_redeliver/2,
         rebalance_module/1,
         is_replicable/1,
         stop/1,
         list_with_minimum_quorum/0,
         drain/1,
         revive/0,
         queue_vm_stats_sups/0,
         queue_vm_ets/0
         ]).

-export([
         added_to_rabbit_registry/2,
         removed_from_rabbit_registry/1,
         known_queue_type_names/0,
         known_queue_type_modules/0
        ]).

-type queue_name() :: rabbit_amqqueue:name().
-type queue_state() :: term().
%% sequence number typically
-type correlation() :: term().
-type arguments() :: queue_arguments | consumer_arguments.
-type queue_type() ::  module().
%% see AMQP 1.0 §2.6.7
-type delivery_count() :: sequence_no().
-type credit() :: uint().

-define(STATE, ?MODULE).

-define(DOWN_KEYS, [name, durable, auto_delete, arguments, pid, type, state]).

-type credit_reply_action() :: {credit_reply, rabbit_types:ctag(), delivery_count(), credit(),
                                Available :: non_neg_integer(), Drain :: boolean()}.

%% anything that the host process needs to do on behalf of the queue type session
-type action() ::
    %% indicate to the queue type module that a message has been delivered
    %% fully to the queue
    {settled, queue_name(), [correlation()]} |
    {deliver, rabbit_types:ctag(), boolean(), [rabbit_amqqueue:qmsg()]} |
    {block | unblock, QueueName :: term()} |
    credit_reply_action() |
    %% credit API v1
    {credit_reply_v1, rabbit_types:ctag(), credit(),
     Available :: non_neg_integer(), Drain :: boolean()}.

-type actions() :: [action()].

-type event() ::
    {down, pid(), Info :: term()} |
    term().

-record(ctx, {module :: module(),
              state :: queue_state()}).

-record(?STATE, {ctxs = #{} :: #{queue_name() => #ctx{}}
                }).

-opaque state() :: #?STATE{}.

%% Delete atom 'credit_api_v1' when feature flag rabbitmq_4.0.0 becomes required.
-type consume_mode() :: {simple_prefetch, Prefetch :: non_neg_integer()} |
                        {credited, Initial :: delivery_count() | credit_api_v1}.
-type consume_spec() :: #{no_ack := boolean(),
                          channel_pid := pid(),
                          limiter_pid => pid() | none,
                          limiter_active => boolean(),
                          mode := consume_mode(),
                          consumer_tag := rabbit_types:ctag(),
                          exclusive_consume => boolean(),
                          args => rabbit_framing:amqp_table(),
                          filter => rabbit_amqp_filter:expression(),
                          ok_msg := term(),
                          acting_user := rabbit_types:username()}.
-type cancel_reason() :: cancel | remove.
-type cancel_spec() :: #{consumer_tag := rabbit_types:ctag(),
                         reason => cancel_reason(),
                         ok_msg => term(),
                         user := rabbit_types:username()}.

-type delivery_options() :: #{correlation => correlation(),
                              atom() => term()}.

-type settle_op() :: complete |
                     requeue |
                     discard |
                     {modify,
                      DeliveryFailed :: boolean(),
                      UndeliverableHere :: boolean(),
                      Annotations :: mc:annotations()}.

-export_type([state/0,
              consume_mode/0,
              consume_spec/0,
              cancel_reason/0,
              cancel_spec/0,
              delivery_options/0,
              credit_reply_action/0,
              action/0,
              actions/0,
              settle_op/0,
              queue_type/0,
              credit/0,
              correlation/0,
              delivery_count/0]).

-callback is_enabled() -> boolean().

-callback is_compatible(Durable :: boolean(),
                        Exclusive :: boolean(),
                        AutoDelete :: boolean()) ->
    boolean().

-callback declare(amqqueue:amqqueue(), node()) ->
    {'new' | 'existing' | 'owner_died', amqqueue:amqqueue()} |
    {'absent', amqqueue:amqqueue(), rabbit_amqqueue:absent_reason()} |
    {'protocol_error', Type :: atom(), Reason :: string(), Args :: term()} |
    {'error', Err :: term() }.

-callback delete(amqqueue:amqqueue(),
                 boolean(),
                 boolean(),
                 rabbit_types:username()) ->
    rabbit_types:ok(non_neg_integer()) |
    rabbit_types:error(in_use | not_empty) |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.

%% checks if the queue should be recovered
-callback is_recoverable(amqqueue:amqqueue()) ->
    boolean().

-callback recover(rabbit_types:vhost(), [amqqueue:amqqueue()]) ->
    {Recovered :: [amqqueue:amqqueue()],
     Failed :: [amqqueue:amqqueue()]}.

-callback purge(amqqueue:amqqueue()) ->
    {ok, non_neg_integer()} | {error, term()}.

-callback policy_changed(amqqueue:amqqueue()) -> ok.

-callback is_stateful() -> boolean().

%% intitialise and return a queue type specific session context
-callback init(amqqueue:amqqueue()) ->
    {ok, queue_state()} | {error, Reason :: term()}.

-callback close(queue_state()) -> ok.
%% update the queue type state from amqqrecord
-callback update(amqqueue:amqqueue(), queue_state()) -> queue_state().

-callback consume(amqqueue:amqqueue(),
                  consume_spec(),
                  queue_state()) ->
    {ok, queue_state(), actions()} |
    {error, Type :: atom(), Format :: string(), FormatArgs :: [term()]}.

-callback cancel(amqqueue:amqqueue(),
                 cancel_spec(),
                 queue_state()) ->
    {ok, queue_state()} | {error, term()}.

%% any async events returned from the queue system should be processed through
%% this
-callback handle_event(queue_name(),
                       Event :: event(),
                       queue_state()) ->
    {ok, queue_state(), actions()} | {error, term()} | {eol, actions()} |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.

-callback deliver([{amqqueue:amqqueue(), queue_state()}],
                  Message :: mc:state(),
                  Options :: delivery_options()) ->
    {[{amqqueue:amqqueue(), queue_state()}], actions()}.

-callback settle(queue_name(), settle_op(), rabbit_types:ctag(),
                 [non_neg_integer()], queue_state()) ->
    {queue_state(), actions()} |
    {'protocol_error', Type :: atom(), Reason :: string(), Args :: term()}.

%% Delete this callback when feature flag rabbitmq_4.0.0 becomes required.
-callback credit_v1(queue_name(), rabbit_types:ctag(), credit(), Drain :: boolean(), queue_state()) ->
    {queue_state(), actions()}.

-callback credit(queue_name(), rabbit_types:ctag(), delivery_count(), credit(),
                 Drain :: boolean(), queue_state()) ->
    {queue_state(), actions()}.

-callback dequeue(queue_name(), NoAck :: boolean(), LimiterPid :: pid(),
                  rabbit_types:ctag(), queue_state()) ->
    {ok, Count :: non_neg_integer(), rabbit_amqqueue:qmsg(), queue_state()} |
    {empty, queue_state()} |
    {error, term()} |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.

%% return a map of state summary information
-callback state_info(queue_state()) ->
    #{atom() := term()}.

%% general queue info
-callback info(amqqueue:amqqueue(), all_keys | rabbit_types:info_keys()) ->
    rabbit_types:infos().

-callback stat(amqqueue:amqqueue()) ->
    {'ok', non_neg_integer(), non_neg_integer()}.

-callback format(amqqueue:amqqueue(), Context :: map()) ->
    [{atom(), term()}].

%% TODO: replace binary() with real types?
-callback capabilities() ->
    #{unsupported_policies := [binary()],
      queue_arguments := [binary()],
      consumer_arguments := [binary()],
      amqp_capabilities => [binary()],
      server_named := boolean(),
      rebalance_module := module() | undefined,
      can_redeliver := boolean(),
      is_replicable := boolean()}.

-callback notify_decorators(amqqueue:amqqueue()) ->
    ok.

-callback policy_apply_to_name() -> binary().

%% -callback on_node_up(node()) -> ok.

%% -callback on_node_down(node()) -> ok.

-callback stop(rabbit_types:vhost()) -> ok.

-callback list_with_minimum_quorum() -> [amqqueue:amqqueue()].

-callback drain([node()]) -> ok.

-callback revive() -> ok.

%% used by rabbit_vm to emit queue process
%% (currently memory and binary) stats
-callback queue_vm_stats_sups() -> {StatsKeys :: [atom()], SupsNames:: [[atom()]]}.

-callback queue_vm_ets() -> {StatsKeys :: [atom()], ETSNames:: [[atom()]]}.

-spec discover(binary() | atom()) -> queue_type().
discover(<<"undefined">>) ->
    fallback();
discover(undefined) ->
    fallback();
discover(TypeDescriptor) ->
    {ok, TypeModule} = rabbit_registry:lookup_type_module(queue, TypeDescriptor),
    TypeModule.

-spec short_alias_of(TypeDescriptor) -> Ret when
      TypeDescriptor :: {utf8, binary()} | atom() | binary(),
      Ret :: binary() | undefined.
%% AMQP 1.0 management client
short_alias_of({utf8, TypeName}) ->
    short_alias_of(TypeName);
short_alias_of(TypeDescriptor) ->
    case rabbit_registry:lookup_type_name(queue, TypeDescriptor) of
        {ok, TypeName} -> TypeName;
        _ -> undefined
    end.

%% If the client does not specify the type, the virtual host does not have any
%% metadata default, and rabbit.default_queue_type is not set in the application env,
%% use this type as the last resort.
-spec fallback() -> queue_type().
fallback() ->
    rabbit_classic_queue.

-spec default() -> queue_type().
default() ->
    V = rabbit_misc:get_env(rabbit,
                            default_queue_type,
                            fallback()),
    rabbit_data_coercion:to_atom(V).

-spec default_alias() -> binary().
default_alias() ->
    short_alias_of(default()).

%% is a specific queue type implementation enabled
-spec is_enabled(module()) -> boolean().
is_enabled(Type) when is_atom(Type) ->
    Type:is_enabled().

-spec is_compatible(module(), boolean(), boolean(), boolean()) ->
    boolean().
is_compatible(Type, Durable, Exclusive, AutoDelete) ->
    Type:is_compatible(Durable, Exclusive, AutoDelete).

-spec declare(amqqueue:amqqueue(), node() | {'ignore_location', node()}) ->
    {'new' | 'existing' | 'owner_died', amqqueue:amqqueue()} |
    {'absent', amqqueue:amqqueue(), rabbit_amqqueue:absent_reason()} |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()} |
    {'error', Type :: atom(), Reason :: string(), Args :: term()} |
    {'error', Err :: term() }.
declare(Q0, Node) ->
    Q = rabbit_queue_decorator:set(rabbit_policy:set(Q0)),
    Mod = amqqueue:get_type(Q),
    case check_queue_limits(Q) of
        ok ->
            Mod:declare(Q, Node);
        Error ->
            Error
    end.

-spec delete(amqqueue:amqqueue(), boolean(),
             boolean(), rabbit_types:username()) ->
    rabbit_types:ok(non_neg_integer()) |
    rabbit_types:error(in_use | not_empty) |
    rabbit_types:error(timeout) |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.
delete(Q, IfUnused, IfEmpty, ActingUser) ->
    Mod = amqqueue:get_type(Q),
    Mod:delete(Q, IfUnused, IfEmpty, ActingUser).

-spec purge(amqqueue:amqqueue()) ->
    {'ok', non_neg_integer()} | {error, term()}.
purge(Q) ->
    Mod = amqqueue:get_type(Q),
    Mod:purge(Q).

-spec policy_changed(amqqueue:amqqueue()) -> 'ok'.
policy_changed(Q) ->
    Mod = amqqueue:get_type(Q),
    Mod:policy_changed(Q).

-spec stat(amqqueue:amqqueue()) ->
    {'ok', non_neg_integer(), non_neg_integer()}.
stat(Q) ->
    Mod = amqqueue:get_type(Q),
    Mod:stat(Q).

-spec format(amqqueue:amqqueue(), map()) ->
    [{atom(), term()}].
format(Q, Context) ->
    Mod = amqqueue:get_type(Q),
    Mod:format(Q, Context).

-spec remove(queue_name(), state()) -> state().
remove(QRef, #?STATE{ctxs = Ctxs0} = State) ->
    case maps:take(QRef, Ctxs0) of
        error ->
            State;
        {#ctx{module = Mod,
              state = S}, Ctxs} ->
            ok = Mod:close(S),
            State#?STATE{ctxs = Ctxs}
    end.

-spec info(amqqueue:amqqueue(), all_keys | rabbit_types:info_keys()) ->
    rabbit_types:infos().
info(Q, Items) when ?amqqueue_state_is(Q, crashed) ->
    info_down(Q, Items, crashed);
info(Q, Items) when ?amqqueue_state_is(Q, stopped) ->
    info_down(Q, Items, stopped);
info(Q, Items) ->
    Mod = amqqueue:get_type(Q),
    Mod:info(Q, Items).

fold_state(Fun, Acc, #?STATE{ctxs = Ctxs}) ->
    maps:fold(Fun, Acc, Ctxs).

state_info(#ctx{state = S,
                module = Mod}) ->
    Mod:state_info(S);
state_info(_) ->
    #{}.

-spec format_status(state()) -> map().
format_status(#?STATE{ctxs = Ctxs}) ->
    #{num_queue_clients => maps:size(Ctxs)}.

down_keys() -> ?DOWN_KEYS.

info_down(Q, DownReason) ->
    info_down(Q, down_keys(), DownReason).

info_down(Q, all_keys, DownReason) ->
    info_down(Q, down_keys(), DownReason);
info_down(Q, Items, DownReason) ->
    [{Item, i_down(Item, Q, DownReason)} || Item <- Items].

i_down(name,               Q, _) -> amqqueue:get_name(Q);
i_down(durable,            Q, _) -> amqqueue:is_durable(Q);
i_down(auto_delete,        Q, _) -> amqqueue:is_auto_delete(Q);
i_down(arguments,          Q, _) -> amqqueue:get_arguments(Q);
i_down(pid,                Q, _) -> amqqueue:get_pid(Q);
i_down(type,               Q, _) -> amqqueue:get_type(Q);
i_down(state, _Q, DownReason)    -> DownReason;
i_down(_K, _Q, _DownReason) -> ''.

is_policy_applicable(Q, Policy) ->
    Mod = amqqueue:get_type(Q),
    Capabilities = Mod:capabilities(),
    NotApplicable = maps:get(unsupported_policies, Capabilities, []),
    lists:all(fun({P, _}) ->
                      not lists:member(P, NotApplicable)
              end, Policy).

-spec is_server_named_allowed(queue_type()) -> boolean().
is_server_named_allowed(Type) ->
    Capabilities = Type:capabilities(),
    maps:get(server_named, Capabilities, false).

-spec amqp_capabilities(queue_type()) ->
    [binary()].
amqp_capabilities(Type) ->
    Capabilities = Type:capabilities(),
    maps:get(?FUNCTION_NAME, Capabilities, []).

-spec arguments(arguments()) -> [binary()].
arguments(ArgumentType) ->
    Args0 = lists:map(fun(T) ->
                              maps:get(ArgumentType, T:capabilities(), [])
                      end, known_queue_type_modules()),
    Args = lists:flatten(Args0),
    lists:usort(Args).

-spec arguments(arguments(), queue_type()) -> [binary()].
arguments(ArgumentType, QueueType) ->
    Capabilities = QueueType:capabilities(),
    maps:get(ArgumentType, Capabilities, []).

notify_decorators(Q) ->
    Mod = amqqueue:get_type(Q),
    Mod:notify_decorators(Q).

-spec init() -> state().
init() ->
    #?STATE{}.

-spec close(state()) -> ok.
close(#?STATE{ctxs = Contexts}) ->
    maps:foreach(fun (_, #ctx{module = Mod,
                              state = S}) ->
                         ok = Mod:close(S)
                 end, Contexts).

-spec new(amqqueue:amqqueue(), state()) -> state().
new(Q, State) when ?is_amqqueue(Q) ->
    Ctx = get_ctx(Q, State),
    set_ctx(Q, Ctx, State).

-spec consume(amqqueue:amqqueue(), consume_spec(), state()) ->
    {ok, state()} |
    {error, Type :: atom(), Format :: string(), FormatArgs :: [term()]}.
consume(Q, Spec, State) ->
    #ctx{state = CtxState0} = Ctx = get_ctx(Q, State),
    Mod = amqqueue:get_type(Q),
    case Mod:consume(Q, Spec, CtxState0) of
        {ok, CtxState} ->
            {ok, set_ctx(Q, Ctx#ctx{state = CtxState}, State)};
        {error, _Type, _Fmt, _FmtArgs} = Err->
            Err
    end.

-spec cancel(amqqueue:amqqueue(),
             cancel_spec(),
             state()) ->
    {ok, state()} | {error, term()}.
cancel(Q, Spec, Ctxs) ->
    #ctx{state = State0} = Ctx = get_ctx(Q, Ctxs),
    Mod = amqqueue:get_type(Q),
    case Mod:cancel(Q, Spec, State0) of
        {ok, State} ->
            {ok, set_ctx(Q, Ctx#ctx{state = State}, Ctxs)};
        Err ->
            Err
    end.

-spec is_recoverable(amqqueue:amqqueue()) ->
    boolean().
is_recoverable(Q) ->
    Mod = amqqueue:get_type(Q),
    Mod:is_recoverable(Q).

-spec recover(rabbit_types:vhost(), [amqqueue:amqqueue()]) ->
    {Recovered :: [amqqueue:amqqueue()],
     Failed :: [amqqueue:amqqueue()]}.
recover(VHost, Qs) ->
    ByType0 = maps:from_keys(known_queue_type_modules(), []),
    ByType = lists:foldl(
               fun (Q, Acc) ->
                       T = amqqueue:get_type(Q),
                       maps:update_with(T, fun (X) ->
                                                   [Q | X]
                                           end, [Q], Acc)
               end, ByType0, Qs),
    maps:fold(fun (Mod, Queues, {R0, F0}) ->
                      {Taken, {R, F}} =  timer:tc(Mod, recover, [VHost, Queues]),
                      rabbit_log:info("Recovering ~b queues of type ~ts took ~bms",
                                      [length(Queues), Mod, Taken div 1000]),
                      {R0 ++ R, F0 ++ F}
              end, {[], []}, ByType).

-spec handle_down(pid(), queue_name(), term(), state()) ->
    {ok, state(), actions()} | {eol, state(), queue_name()} | {error, term()}.
handle_down(Pid, QName, Info, State0) ->
    case handle_event(QName, {down, Pid, Info}, State0) of
        {ok, State, Actions} ->
            {ok, State, Actions};
        {eol, []} ->
            {eol, State0, QName};
        Err ->
            Err
    end.

%% messages sent from queues
-spec handle_event(queue_name(), term(), state()) ->
    {ok, state(), actions()} | {eol, actions()} | {error, term()} |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.
handle_event(QRef, Evt, Ctxs) ->
    %% events can arrive after a queue state has been cleared up
    %% so need to be defensive here
    case get_ctx(QRef, Ctxs, undefined) of
        #ctx{module = Mod,
             state = State0} = Ctx  ->
            case Mod:handle_event(QRef, Evt, State0) of
                {ok, State, Actions} ->
                    {ok, set_ctx(QRef, Ctx#ctx{state = State}, Ctxs), Actions};
                Other ->
                    Other
            end;
        undefined ->
            {ok, Ctxs, []}
    end.

-spec module(queue_name(), state()) ->
    {ok, module()} | {error, not_found}.
module(QRef, State) ->
    %% events can arrive after a queue state has been cleared up
    %% so need to be defensive here
    case get_ctx(QRef, State, undefined) of
        #ctx{module = Mod} ->
            {ok, Mod};
        undefined ->
            {error, not_found}
    end.

%% convenience function for throwaway publishes
-spec publish_at_most_once(rabbit_types:exchange() |
                           rabbit_exchange:name(),
                           mc:state()) ->
    ok | {error, not_found}.
publish_at_most_once(#resource{} = XName, Msg) ->
    case rabbit_exchange:lookup(XName) of
        {ok, X} ->
            publish_at_most_once(X, Msg);
        Err ->
            Err
    end;
publish_at_most_once(X, Msg)
    when element(1, X) == exchange -> % hacky but good enough
    QNames = rabbit_exchange:route(X, Msg, #{return_binding_keys => true}),
    Qs = rabbit_amqqueue:lookup_many(QNames),
    _ = deliver(Qs, Msg, #{}, stateless),
    ok.

-spec deliver([amqqueue:amqqueue() |
               {amqqueue:amqqueue(), rabbit_exchange:route_infos()}],
              Message :: mc:state(),
              delivery_options(),
              stateless | state()) ->
    {ok, state(), actions()} | {error, Reason :: term()}.
deliver(Qs, Message, Options, State) ->
    try
        deliver0(Qs, Message, Options, State)
    catch
        exit:Reason ->
            {error, Reason}
    end.

deliver0(Qs, Message0, Options, stateless) ->
    ByTypeAndBindingKeys =
    lists:foldl(fun(Elem, Acc) ->
                        {Q, BKeys} = queue_binding_keys(Elem),
                        Mod = amqqueue:get_type(Q),
                        maps:update_with(
                          {Mod, BKeys}, fun(A) ->
                                                [{Q, stateless} | A]
                                        end, [{Q, stateless}], Acc)
                end, #{}, Qs),
    maps:foreach(fun({Mod, BKeys}, QSs) ->
                         Message = add_binding_keys(Message0, BKeys),
                         _ = Mod:deliver(QSs, Message, Options)
                 end, ByTypeAndBindingKeys),
    {ok, stateless, []};
deliver0(Qs, Message0, Options, #?STATE{} = State0) ->
    %% TODO: optimise single queue case?
    %% sort by queue type - then dispatch each group
    ByTypeAndBindingKeys =
    lists:foldl(
      fun (Elem, Acc) ->
              {Q, BKeys} = queue_binding_keys(Elem),
              Mod = amqqueue:get_type(Q),
              QState = case Mod:is_stateful() of
                           true ->
                               #ctx{state = S} = get_ctx(Q, State0),
                               S;
                           false ->
                               stateless
                       end,
              maps:update_with(
                {Mod, BKeys}, fun (A) ->
                                      [{Q, QState} | A]
                              end, [{Q, QState}], Acc)
      end, #{}, Qs),
    %%% dispatch each group to queue type interface?
    {Xs, Actions} = maps:fold(
                      fun({Mod, BKeys}, QSs, {X0, A0}) ->
                              Message = add_binding_keys(Message0, BKeys),
                              {X, A} = Mod:deliver(QSs, Message, Options),
                              {X0 ++ X, A0 ++ A}
                      end, {[], []}, ByTypeAndBindingKeys),
    State = lists:foldl(
              fun({Q, S}, Acc) ->
                      Ctx = get_ctx_with(Q, Acc, S),
                      set_ctx(qref(Q), Ctx#ctx{state = S}, Acc)
              end, State0, Xs),
    {ok, State, Actions}.

queue_binding_keys(Q)
  when ?is_amqqueue(Q) ->
    {Q, #{}};
queue_binding_keys({Q, #{binding_keys := BindingKeys}})
  when ?is_amqqueue(Q) andalso is_map(BindingKeys) ->
    {Q, BindingKeys};
queue_binding_keys({Q, _RouteInfos})
  when ?is_amqqueue(Q) ->
    {Q, #{}}.

add_binding_keys(Message, BindingKeys)
  when map_size(BindingKeys) =:= 0 ->
    Message;
add_binding_keys(Message, BindingKeys) ->
    mc:set_annotation(binding_keys, maps:keys(BindingKeys), Message).

-spec settle(queue_name(), settle_op(), rabbit_types:ctag(),
             [non_neg_integer()], state()) ->
          {ok, state(), actions()} |
          {'protocol_error', Type :: atom(), Reason :: string(), Args :: term()}.
settle(#resource{kind = queue} = QRef, Op, CTag, MsgIds, Ctxs) ->
    case get_ctx(QRef, Ctxs, undefined) of
        undefined ->
            %% if we receive a settlement and there is no queue state it means
            %% the queue was deleted with active consumers
            {ok, Ctxs, []};
        #ctx{state = State0,
             module = Mod} = Ctx ->
            case Mod:settle(QRef, Op, CTag, MsgIds, State0) of
                {State, Actions} ->
                    {ok, set_ctx(QRef, Ctx#ctx{state = State}, Ctxs), Actions};
                Err ->
                    Err
            end
    end.

%% Delete this function when feature flag rabbitmq_4.0.0 becomes required.
-spec credit_v1(queue_name(), rabbit_types:ctag(), credit(), boolean(), state()) ->
    {ok, state(), actions()}.
credit_v1(QName, CTag, LinkCreditSnd, Drain, Ctxs) ->
    #ctx{state = State0,
         module = Mod} = Ctx = get_ctx(QName, Ctxs),
    {State, Actions} = Mod:credit_v1(QName, CTag, LinkCreditSnd, Drain, State0),
    {ok, set_ctx(QName, Ctx#ctx{state = State}, Ctxs), Actions}.

%% credit API v2
-spec credit(queue_name(), rabbit_types:ctag(), delivery_count(), credit(), boolean(), state()) ->
    {ok, state(), actions()}.
credit(QName, CTag, DeliveryCount, Credit, Drain, Ctxs) ->
    #ctx{state = State0,
         module = Mod} = Ctx = get_ctx(QName, Ctxs),
    {State, Actions} = Mod:credit(QName, CTag, DeliveryCount, Credit, Drain, State0),
    {ok, set_ctx(QName, Ctx#ctx{state = State}, Ctxs), Actions}.

-spec dequeue(amqqueue:amqqueue(), boolean(),
              pid(), rabbit_types:ctag(), state()) ->
    {ok, non_neg_integer(), term(), state()}  |
    {empty, state()} | rabbit_types:error(term()) |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.
dequeue(Q, NoAck, LimiterPid, CTag, Ctxs) ->
    #ctx{state = State0} = Ctx = get_ctx(Q, Ctxs),
    Mod = amqqueue:get_type(Q),
    QName = amqqueue:get_name(Q),
    case Mod:dequeue(QName, NoAck, LimiterPid, CTag, State0) of
        {ok, Num, Msg, State} ->
            {ok, Num, Msg, set_ctx(Q, Ctx#ctx{state = State}, Ctxs)};
        {empty, State} ->
            {empty, set_ctx(Q, Ctx#ctx{state = State}, Ctxs)};
        {error, _} = Err ->
            Err;
        {timeout, _} = Err ->
            {error, Err};
        {protocol_error, _, _, _} = Err ->
            Err
    end.


-spec added_to_rabbit_registry(atom(), atom()) -> ok.
added_to_rabbit_registry(_Type, _ModuleName) -> ok.

-spec removed_from_rabbit_registry(atom()) -> ok.
removed_from_rabbit_registry(_Type) -> ok.

get_ctx(QOrQref, State) ->
    get_ctx_with(QOrQref, State, undefined).

get_ctx_with(Q, #?STATE{ctxs = Contexts}, InitState)
  when ?is_amqqueue(Q) ->
    Ref = qref(Q),
    case Contexts of
        #{Ref := #ctx{module = Mod,
                      state = State} = Ctx} ->
            Ctx#ctx{state = Mod:update(Q, State)};
        _ when InitState == undefined ->
            %% not found and no initial state passed - initialize new state
            Mod = amqqueue:get_type(Q),
            case Mod:init(Q) of
                {error, Reason} ->
                    exit({Reason, Ref});
                {ok, QState} ->
                    #ctx{module = Mod,
                         state = QState}
            end;
        _  ->
            %% not found - initialize with supplied initial state
            Mod = amqqueue:get_type(Q),
            #ctx{module = Mod,
                 state = InitState}
    end;
get_ctx_with(#resource{kind = queue} = QRef, Contexts, undefined) ->
    case get_ctx(QRef, Contexts, undefined) of
        undefined ->
            exit({queue_context_not_found, QRef});
        Ctx ->
            Ctx
    end.

get_ctx(QRef, #?STATE{ctxs = Contexts}, Default) ->
    Ref = qref(QRef),
    %% if we use a QRef it should always be initialised
    maps:get(Ref, Contexts, Default).

set_ctx(QRef, Ctx, #?STATE{ctxs = Contexts} = State) ->
    Ref = qref(QRef),
    State#?STATE{ctxs = maps:put(Ref, Ctx, Contexts)}.

qref(#resource{kind = queue} = QName) ->
    QName;
qref(Q) when ?is_amqqueue(Q) ->
    amqqueue:get_name(Q).

-spec known_queue_type_modules() -> [module()].
known_queue_type_modules() ->
    Registered = rabbit_registry:lookup_all(queue),
    {_, Modules} = lists:unzip(Registered),
    Modules.

-spec known_queue_type_names() -> [binary()].
known_queue_type_names() ->
    Registered = rabbit_registry:lookup_all(queue),
    {QueueTypes, _} = lists:unzip(Registered),
    lists:map(fun(X) -> atom_to_binary(X) end, QueueTypes).

inject_dqt(VHost) when ?is_vhost(VHost) ->
    inject_dqt(vhost:to_map(VHost));
inject_dqt(VHost) when is_list(VHost) ->
    inject_dqt(rabbit_data_coercion:to_map(VHost));
inject_dqt(M = #{default_queue_type := undefined}) ->
    NQT = short_alias_of(default()),
    Meta0 = maps:get(metadata, M, #{}),
    Meta = Meta0#{default_queue_type => NQT},

    M#{default_queue_type => NQT, metadata => Meta};
inject_dqt(M = #{default_queue_type := DQT}) ->
    NQT = short_alias_of(DQT),
    Meta0 = maps:get(metadata, M, #{}),
    Meta = Meta0#{default_queue_type => NQT},

    M#{default_queue_type => NQT, metadata => Meta}.

-spec vhosts_with_dqt([any()]) -> [map()].
vhosts_with_dqt(List) when is_list(List) ->
    %% inject DQT (default queue type) at the top level and
    %% its metadata
    lists:map(fun inject_dqt/1, List).

-spec check_queue_limits(amqqueue:amqqueue()) ->
          ok |
          {error, queue_limit_exceeded, Reason :: string(), Args :: term()}.
check_queue_limits(Q) ->
    maybe
        ok ?= check_vhost_queue_limit(Q),
        ok ?= check_cluster_queue_limit(Q)
    end.

check_vhost_queue_limit(Q) ->
    #resource{name = QueueName} = amqqueue:get_name(Q),
    VHost = amqqueue:get_vhost(Q),
    case rabbit_vhost_limit:is_over_queue_limit(VHost) of
        false ->
            ok;
        {true, Limit} ->
            queue_limit_error("cannot declare queue '~ts': "
                              "queue limit in vhost '~ts' (~tp) is reached",
                              [QueueName, VHost, Limit])
    end.

check_cluster_queue_limit(Q) ->
    #resource{name = QueueName} = amqqueue:get_name(Q),
    case rabbit_misc:get_env(rabbit, cluster_queue_limit, infinity) of
        infinity ->
            ok;
        Limit ->
            case rabbit_db_queue:count() >= Limit of
                true ->
                    queue_limit_error("cannot declare queue '~ts': "
                                      "queue limit in cluster (~tp) is reached",
                                      [QueueName, Limit]);
                false ->
                    ok
            end
    end.

queue_limit_error(Reason, ReasonArgs) ->
    {error, queue_limit_exceeded, Reason, ReasonArgs}.

-spec can_redeliver(queue_name(), state()) ->
    {ok, module()} | {error, not_found}.
can_redeliver(Q, State) ->
    case module(Q, State) of
        {ok, TypeModule} ->
            Capabilities = TypeModule:capabilities(),
            maps:get(can_redeliver, Capabilities, false);
        _ -> false
    end.

-spec rebalance_module(amqqueue:amqqueue()) -> undefine | module().
rebalance_module(Q) ->
    TypeModule = amqqueue:get_type(Q),
    Capabilities = TypeModule:capabilities(),
    maps:get(rebalance_module, Capabilities, undefined).

-spec is_replicable(amqqueue:amqqueue()) -> undefine | module().
is_replicable(Q) ->
    TypeModule = amqqueue:get_type(Q),
    Capabilities = TypeModule:capabilities(),
    maps:get(is_replicable, Capabilities, false).

-spec stop(rabbit_types:vhost()) -> ok.
stop(VHost) ->
    %% original rabbit_amqqueue:stop doesn't do any catches or try after
    _ = [TypeModule:stop(VHost) || {_Type, TypeModule} <- rabbit_registry:lookup_all(queue)],
    ok.

list_with_minimum_quorum() ->
    lists:append([TypeModule:list_with_minimum_quorum()
                  || {_Type, TypeModule} <- rabbit_registry:lookup_all(queue)]).

drain(TransferCandidates) ->
    _ = [TypeModule:drain(TransferCandidates) ||
            {_Type, TypeModule} <- rabbit_registry:lookup_all(queue)],
    ok.

revive() ->
    _ = [TypeModule:revive() ||
            {_Type, TypeModule} <- rabbit_registry:lookup_all(queue)],
    ok.

queue_vm_stats_sups() ->
    lists:foldl(fun({_TypeName, TypeModule}, {KeysAcc, SupsAcc}) ->
                        {Keys, Sups} = TypeModule:queue_vm_stats_sups(),
                        {KeysAcc ++ Keys, SupsAcc ++ Sups}
                end,
                {[], []}, rabbit_registry:lookup_all(queue)).

queue_vm_ets() ->
    lists:foldl(fun({_TypeName, TypeModule}, {KeysAcc, SupsAcc}) ->
                        {Keys, Tables} = TypeModule:queue_vm_ets(),
                        {KeysAcc ++ Keys, SupsAcc ++ Tables}
                end,
                {[], []}, rabbit_registry:lookup_all(queue)).
