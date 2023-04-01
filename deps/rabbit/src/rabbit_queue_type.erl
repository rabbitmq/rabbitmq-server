%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_queue_type).
-include("amqqueue.hrl").
-include_lib("rabbit_common/include/resource.hrl").

-export([
         init/0,
         close/1,
         discover/1,
         feature_flag_name/1,
         default/0,
         is_enabled/1,
         is_compatible/4,
         declare/2,
         delete/4,
         is_recoverable/1,
         recover/2,
         purge/1,
         policy_changed/1,
         stat/1,
         remove/2,
         info/2,
         state_info/1,
         format_status/1,
         info_down/2,
         info_down/3,
         %% stateful client API
         new/2,
         consume/3,
         cancel/5,
         handle_down/4,
         handle_event/3,
         module/2,
         deliver/3,
         settle/5,
         credit/5,
         dequeue/5,
         fold_state/3,
         is_policy_applicable/2,
         is_server_named_allowed/1,
         arguments/1,
         arguments/2,
         notify_decorators/1
         ]).

-type queue_name() :: rabbit_amqqueue:name().
-type queue_state() :: term().
-type msg_tag() :: term().
-type arguments() :: queue_arguments | consumer_arguments.
-type queue_type() :: rabbit_classic_queue | rabbit_quorum_queue | rabbit_stream_queue.

-export_type([queue_type/0]).

-define(STATE, ?MODULE).

%% Recoverable mirrors shouldn't really be a generic one, but let's keep it here until
%% mirrored queues are deprecated.
-define(DOWN_KEYS, [name, durable, auto_delete, arguments, pid, recoverable_slaves, type, state]).

%% TODO resolve all registered queue types from registry
-define(QUEUE_TYPES, [rabbit_classic_queue, rabbit_quorum_queue, rabbit_stream_queue]).

%% anything that the host process needs to do on behalf of the queue type session
-type action() ::
    %% indicate to the queue type module that a message has been delivered
    %% fully to the queue
    {settled, Success :: boolean(), [msg_tag()]} |
    {deliver, rabbit_types:ctag(), boolean(), [rabbit_amqqueue:qmsg()]} |
    {block | unblock, QueueName :: term()}.

-type actions() :: [action()].

-type event() ::
    {down, pid(), Info :: term()} |
    term().

-record(ctx, {module :: module(),
              %% "publisher confirm queue accounting"
              %% queue type implementation should emit a:
              %% {settle, Success :: boolean(), msg_tag()}
              %% to either settle or reject the delivery of a
              %% message to the queue instance
              %% The queue type module will then emit a {confirm | reject, [msg_tag()}
              %% action to the channel or channel like process when a msg_tag
              %% has reached its conclusion
              state :: queue_state()}).


-record(?STATE, {ctxs = #{} :: #{queue_name() => #ctx{}}
                }).

-opaque state() :: #?STATE{}.

-type consume_spec() :: #{no_ack := boolean(),
                          channel_pid := pid(),
                          limiter_pid => pid() | none,
                          limiter_active => boolean(),
                          prefetch_count => non_neg_integer(),
                          consumer_tag := rabbit_types:ctag(),
                          exclusive_consume => boolean(),
                          args => rabbit_framing:amqp_table(),
                          ok_msg := term(),
                          acting_user :=  rabbit_types:username()}.



-type settle_op() :: 'complete' | 'requeue' | 'discard'.

-export_type([state/0,
              consume_spec/0,
              action/0,
              actions/0,
              settle_op/0]).

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
-callback init(amqqueue:amqqueue()) -> {ok, queue_state()} | {error, Reason :: term()}.

-callback close(queue_state()) -> ok.
%% update the queue type state from amqqrecord
-callback update(amqqueue:amqqueue(), queue_state()) -> queue_state().

-callback consume(amqqueue:amqqueue(),
                  consume_spec(),
                  queue_state()) ->
    {ok, queue_state(), actions()} | {error, term()} |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.

-callback cancel(amqqueue:amqqueue(),
                 rabbit_types:ctag(),
                 term(),
                 rabbit_types:username(),
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
                  Delivery :: term()) ->
    {[{amqqueue:amqqueue(), queue_state()}], actions()}.

-callback settle(queue_name(), settle_op(), rabbit_types:ctag(),
                 [non_neg_integer()], queue_state()) ->
    {queue_state(), actions()} |
    {'protocol_error', Type :: atom(), Reason :: string(), Args :: term()}.

-callback credit(queue_name(), rabbit_types:ctag(),
                 non_neg_integer(), Drain :: boolean(), queue_state()) ->
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

-callback capabilities() ->
    #{atom() := term()}.

-callback notify_decorators(amqqueue:amqqueue()) ->
    ok.

%% TODO: should this use a registry that's populated on boot?
discover(<<"quorum">>) ->
    rabbit_quorum_queue;
discover(<<"classic">>) ->
    rabbit_classic_queue;
discover(<<"stream">>) ->
    rabbit_stream_queue.

feature_flag_name(<<"quorum">>) ->
    quorum_queue;
feature_flag_name(<<"classic">>) ->
    undefined;
feature_flag_name(<<"stream">>) ->
    stream_queue;
feature_flag_name(_) ->
    undefined.

default() ->
    rabbit_classic_queue.

%% is a specific queue type implementation enabled
-spec is_enabled(module()) -> boolean().
is_enabled(Type) ->
    Type:is_enabled().

-spec is_compatible(module(), boolean(), boolean(), boolean()) ->
    boolean().
is_compatible(Type, Durable, Exclusive, AutoDelete) ->
    Type:is_compatible(Durable, Exclusive, AutoDelete).

-spec declare(amqqueue:amqqueue(), node() | {'ignore_location', node()}) ->
    {'new' | 'existing' | 'owner_died', amqqueue:amqqueue()} |
    {'absent', amqqueue:amqqueue(), rabbit_amqqueue:absent_reason()} |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()} |
    {'error', Err :: term() }.
declare(Q0, Node) ->
    Q = rabbit_queue_decorator:set(rabbit_policy:set(Q0)),
    Mod = amqqueue:get_type(Q),
    Mod:declare(Q, Node).

-spec delete(amqqueue:amqqueue(), boolean(),
             boolean(), rabbit_types:username()) ->
    rabbit_types:ok(non_neg_integer()) |
    rabbit_types:error(in_use | not_empty) |
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

-spec remove(queue_name(), state()) -> state().
remove(QRef, #?STATE{ctxs = Ctxs0} = State) ->
    case maps:take(QRef, Ctxs0) of
        error ->
            State;
        {_, Ctxs} ->
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
i_down(recoverable_slaves, Q, _) -> amqqueue:get_recoverable_slaves(Q);
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

-spec arguments(arguments()) -> [binary()].
arguments(ArgumentType) ->
    Args0 = lists:map(fun(T) ->
                              maps:get(ArgumentType, T:capabilities(), [])
                      end, ?QUEUE_TYPES),
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
    maps:foreach(
      fun (_, #ctx{module = Mod,
                   state = S}) ->
              ok = Mod:close(S)
      end, Contexts).

-spec new(amqqueue:amqqueue(), state()) -> state().
new(Q, State) when ?is_amqqueue(Q) ->
    Ctx = get_ctx(Q, State),
    set_ctx(Q, Ctx, State).

-spec consume(amqqueue:amqqueue(), consume_spec(), state()) ->
    {ok, state()} | {error, term()}.
consume(Q, Spec, State) ->
    #ctx{state = CtxState0} = Ctx = get_ctx(Q, State),
    Mod = amqqueue:get_type(Q),
    case Mod:consume(Q, Spec, CtxState0) of
        {ok, CtxState} ->
            {ok, set_ctx(Q, Ctx#ctx{state = CtxState}, State)};
        Err ->
            Err
    end.

%% TODO switch to cancel spec api
-spec cancel(amqqueue:amqqueue(),
             rabbit_types:ctag(),
             term(),
             rabbit_types:username(),
             state()) ->
    {ok, state()} | {error, term()}.
cancel(Q, Tag, OkMsg, ActiveUser, Ctxs) ->
    #ctx{state = State0} = Ctx = get_ctx(Q, Ctxs),
    Mod = amqqueue:get_type(Q),
    case Mod:cancel(Q, Tag, OkMsg, ActiveUser, State0) of
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
    ByType0 = maps:from_keys(?QUEUE_TYPES, []),
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

-spec deliver([amqqueue:amqqueue()], Delivery :: term(),
              stateless | state()) ->
    {ok, state(), actions()} | {error, Reason :: term()}.
deliver(Qs, Delivery, State) ->
    try
        deliver0(Qs, Delivery, State)
    catch
        exit:Reason ->
            {error, Reason}
    end.

deliver0(Qs, Delivery, stateless) ->
    ByType = lists:foldl(fun(Q, Acc) ->
                                 Mod = amqqueue:get_type(Q),
                                 maps:update_with(
                                   Mod, fun(A) ->
                                                [{Q, stateless} | A]
                                        end, [{Q, stateless}], Acc)
                         end, #{}, Qs),
    maps:foreach(fun(Mod, QSs) ->
                         _ = Mod:deliver(QSs, Delivery)
                 end, ByType),
    {ok, stateless, []};
deliver0(Qs, Delivery, #?STATE{} = State0) ->
    %% TODO: optimise single queue case?
    %% sort by queue type - then dispatch each group
    ByType = lists:foldl(
               fun (Q, Acc) ->
                       Mod = amqqueue:get_type(Q),
                       QState = case Mod:is_stateful() of
                                    true ->
                                        #ctx{state = S} = get_ctx(Q, State0),
                                        S;
                                    false ->
                                        stateless
                                end,
                       maps:update_with(
                         Mod, fun (A) ->
                                      [{Q, QState} | A]
                              end, [{Q, QState}], Acc)
               end, #{}, Qs),
    %%% dispatch each group to queue type interface?
    {Xs, Actions} = maps:fold(fun(Mod, QSs, {X0, A0}) ->
                                      {X, A} = Mod:deliver(QSs, Delivery),
                                      {X0 ++ X, A0 ++ A}
                              end, {[], []}, ByType),
    State = lists:foldl(
              fun({Q, S}, Acc) ->
                      Ctx = get_ctx_with(Q, Acc, S),
                      set_ctx(qref(Q), Ctx#ctx{state = S}, Acc)
              end, State0, Xs),
    {ok, State, Actions}.

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

-spec credit(amqqueue:amqqueue() | queue_name(),
             rabbit_types:ctag(), non_neg_integer(),
             boolean(), state()) -> {ok, state(), actions()}.
credit(Q, CTag, Credit, Drain, Ctxs) ->
    #ctx{state = State0,
         module = Mod} = Ctx = get_ctx(Q, Ctxs),
    QName = amqqueue:get_name(Q),
    {State, Actions} = Mod:credit(QName, CTag, Credit, Drain, State0),
    {ok, set_ctx(Q, Ctx#ctx{state = State}, Ctxs), Actions}.

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
