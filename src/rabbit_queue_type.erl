-module(rabbit_queue_type).
-include("amqqueue.hrl").

-export([
         discover/1,
         default/0,
         is_enabled/1,
         declare/2,
         delete/4,
         stat/1,
         new/2,
         consume/3,
         cancel/6,
         handle_event/3,
         deliver/3,
         settle/5,
         reject/5,
         credit/5,
         dequeue/5,

         name/2
         ]).

%% temporary
-export([with/3]).

-type queue_ref() :: pid() | atom().
-type queue_name() :: rabbit_types:r(queue).
-type queue_state() :: term().

%% anything that the host process needs to do on behalf of the queue type
%% session, like knowing when to notify on monitor down
-type action() ::
    {monitor, Pid :: pid(), queue_ref()} |
    {deliver, rabbit_type:ctag(), boolean(), [rabbit_amqqueue:qmsg()]}.

-type actions() :: [action()].

-record(ctx, {module :: module(),
              name :: queue_name(),
              state :: queue_state()}).

-opaque ctxs() :: #{queue_ref() => #ctx{}}.

-type consume_spec() :: #{no_ack := boolean(),
                          channel_pid := pid(),
                          limiter_pid => pid(),
                          limiter_active => boolean(),
                          prefetch_count => non_neg_integer(),
                          consumer_tag := rabbit_types:ctag(),
                          exclusive_consume => boolean(),
                          args => rabbit_framing:amqp_table(),
                          ok_msg := term(),
                          acting_user :=  rabbit_types:username()}.


-export_type([ctxs/0,
              consume_spec/0,
              action/0,
              actions/0]).


% copied from rabbit_amqqueue
-type absent_reason() :: 'nodedown' | 'crashed' | stopped | timeout.

%% intitialise and return a queue type specific session context
-callback init(amqqueue:amqqueue()) -> term().

%% is the queue type feature enabled
-callback is_enabled() -> boolean().

-callback declare(amqqueue:amqqueue(), node()) ->
    {'new' | 'existing' | 'owner_died', amqqueue:amqqueue()} |
    {'absent', amqqueue:amqqueue(), absent_reason()} |
    rabbit_types:channel_exit().

-callback delete(amqqueue:amqqueue(),
                 boolean(),
                 boolean(),
                 rabbit_types:username()) ->
    rabbit_types:ok(non_neg_integer()) |
    rabbit_types:error(in_use | not_empty).

-callback consume(amqqueue:amqqueue(),
                  consume_spec(),
                  queue_state()) ->
    {ok, queue_state(), actions()} | {error, term()}.

-callback cancel(amqqueue:amqqueue(), pid(),
             rabbit_types:ctag(),
             term(),
             rabbit_types:username(),
             queue_state()) ->
    {ok, queue_state(), actions()} | {error, term()}.

-callback handle_event(Event :: term(),
                       queue_state()) ->
    {ok, queue_state(), actions()} | {error, term()} | eol.

-callback deliver([{amqqueue:amqqueue(), queue_state()}],
                  Delivery :: term()) ->
    {[{amqqueue:amqqueue(), queue_state()}], actions()}.

-callback settle(rabbit_types:ctag(), [non_neg_integer()],
                 ChPid :: pid(), queue_state()) ->
    queue_state().

-callback reject(rabbit_types:ctag(), Requeue :: boolean(),
                 MsgIds :: [non_neg_integer()], queue_state()) ->
    queue_state().

-callback credit(rabbit_types:ctag(),
                 non_neg_integer(), Drain :: boolean(), queue_state()) ->
    queue_state().

-callback dequeue(NoAck :: boolean(), LimiterPid :: pid(),
                  rabbit_types:ctag(), queue_state()) ->
    {ok, Count :: non_neg_integer(), empty | rabbit_amqqueue:qmsg(),
     queue_state()}.

%% TODO: this should be controlled by a registry that is populated on boot
discover(<<"quorum">>) ->
    rabbit_quorum_queue;
discover(<<"classic">>) ->
    rabbit_classic_queue.

default() ->
    rabbit_classic_queue.

-spec is_enabled(module()) -> boolean().
is_enabled(Type) ->
    Type:is_enabled().

-spec declare(amqqueue:amqqueue(), node()) ->
    {'new' | 'existing' | 'owner_died', amqqueue:amqqueue()} |
    {'absent', amqqueue:amqqueue(), absent_reason()} |
    rabbit_types:channel_exit().
declare(Q, Node) ->
    Mod = amqqueue:get_type(Q),
    Mod:declare(Q, Node).

-spec delete(amqqueue:amqqueue(), boolean(),
             boolean(), rabbit_types:username()) ->
    rabbit_types:ok(non_neg_integer()) |
    rabbit_types:error(in_use | not_empty).
delete(Q, IfUnused, IfEmpty, ActingUser) ->
    Mod = amqqueue:get_type(Q),
    Mod:delete(Q, IfUnused, IfEmpty, ActingUser).


-spec stat(amqqueue:amqqueue()) ->
    {'ok', non_neg_integer(), non_neg_integer()}.
stat(Q) ->
    Mod = amqqueue:get_type(Q),
    Mod:stat(Q).

-spec new(amqqueue:amqqueue(), ctxs()) -> ctxs().
new(Q, Ctxs) when ?is_amqqueue(Q) ->
    Mod = amqqueue:get_type(Q),
    Name = amqqueue:get_name(Q),
    Ctx = #ctx{module = Mod,
               name = Name,
               state = Mod:init(Q)},
    Ctxs#{qref(Q) => Ctx}.

-spec consume(amqqueue:amqqueue(), consume_spec(), ctxs()) ->
    {ok, ctxs(), actions()} | {error, term()}.
consume(Q, Spec, Ctxs) ->
    #ctx{state = State0} = Ctx = get_ctx(Q, Ctxs),
    Mod = amqqueue:get_type(Q),
    case Mod:consume(Q, Spec, State0) of
        {ok, State, Actions} ->
            {ok, set_ctx(Q, Ctx#ctx{state = State}, Ctxs), Actions};
        Err ->
            Err
    end.

%% TODO switch to cancel spec api
-spec cancel(amqqueue:amqqueue(), pid(),
             rabbit_types:ctag(),
             term(),
             rabbit_types:username(),
             ctxs()) ->
    {ok, ctxs(), actions()} | {error, term()}.
cancel(Q, ChPid, Tag, OkMsg, ActiveUser, Ctxs) ->
    #ctx{state = State0} = Ctx = get_ctx(Q, Ctxs),
    Mod = amqqueue:get_type(Q),
    case Mod:cancel(Q, ChPid, Tag, OkMsg, ActiveUser, State0) of
        {ok, State} ->
            {ok, set_ctx(Q, Ctx#ctx{state = State}, Ctxs)};
        Err ->
            Err
    end.

%% messages sent from queues
-spec handle_event(queue_ref(), term(), ctxs()) ->
    {ok, ctxs(), actions()} | {error, term()}.
handle_event(QRef, Evt, Ctxs) ->
    %% events can arrive after a queue state has been cleared up
    %% so need to be defensive here
    case get_ctx(QRef, Ctxs) of
        #ctx{module = Mod,
             state = State0} = Ctx  ->
            case Mod:handle_event(Evt, State0) of
                {ok, State, Actions} ->
                    {ok, set_ctx(QRef, Ctx#ctx{state = State}, Ctxs), Actions};
                Err ->
                    Err
            end;
        undefined ->
            {ok, Ctxs, []}
    end.


deliver(Qs, Delivery, Ctxs) ->
    %% sort by queue type - then dispatch each group
    ByType = lists:foldl(fun (Q, Acc) ->
                                 T = amqqueue:get_type(Q),
                                 Ctx = get_ctx(Q, Ctxs),
                                 maps:update_with(
                                   T, fun (A) ->
                                              Ctx = get_ctx(Q, Ctxs),
                                              [{Q, Ctx#ctx.state} | A]
                                      end, [{Q, Ctx#ctx.state}], Acc)
                         end, #{}, Qs),
    %%% dispatch each group to queue type interface?
    {Xs, Actions} = maps:fold(fun(Mod, QSs, {X0, A0}) ->
                                      {X, A} = Mod:deliver(QSs, Delivery),
                                      {X0 ++ X, A0 ++ A}
                              end, {[], []}, ByType),
    {lists:foldl(
       fun({Q, S}, Acc) ->
               Ctx = get_ctx(Q, Acc),
               set_ctx(qref(Q), Ctx#ctx{state = S}, Acc)
       end, Ctxs, Xs), Actions}.


settle(QRef, CTag, MsgIds, ChPid, Ctxs) ->
    #ctx{state = State0,
         module = Mod} = Ctx = get_ctx(QRef, Ctxs),
    State = Mod:settle(CTag, MsgIds, ChPid, State0),
    set_ctx(QRef, Ctx#ctx{state = State}, Ctxs).

reject(QRef, CTag, Requeue, MsgIds, Ctxs) ->
    #ctx{state = State0,
         module = Mod} = Ctx = get_ctx(QRef, Ctxs),
    State = Mod:reject(CTag, Requeue, MsgIds, State0),
    set_ctx(QRef, Ctx#ctx{state = State}, Ctxs).

credit(Q, CTag, Credit, Drain, Ctxs) ->
    #ctx{state = State0,
         module = Mod} = Ctx = get_ctx(Q, Ctxs),
    State = Mod:settle(CTag, Credit, Drain, State0),
    set_ctx(Q, Ctx#ctx{state = State}, Ctxs).

dequeue(Q, NoAck, LimiterPid, CTag, Ctxs) ->
    #ctx{state = State0} = Ctx = get_ctx(Q, Ctxs),
    Mod = amqqueue:get_type(Q),
    case Mod:dequeue(NoAck, LimiterPid, CTag, State0) of
        {ok, Num, Msg, State} ->
            {ok, Num, Msg, set_ctx(Q, Ctx#ctx{state = State}, Ctxs)};
        {empty, State} ->
            {empty, set_ctx(Q, Ctx#ctx{state = State}, Ctxs)}
    end.

name(QRef, Ctxs) ->
    case Ctxs of
        #{QRef := Ctx} ->
            Ctx#ctx.name;
        _ ->
            undefined
    end.

%% temporary
with(QRef, Fun, Ctxs) ->
    #ctx{state = State0} = Ctx = get_ctx(QRef, Ctxs),
    {Res, State} = Fun(State0),
    {Res, set_ctx(QRef, Ctx#ctx{state = State}, Ctxs)}.


get_ctx(Q, Contexts) when ?is_amqqueue(Q) ->
    Ref = qref(Q),
    case Contexts of
        #{Ref := Ctx} ->
            Ctx;
        _ ->
            %% not found - initialize
            Mod = amqqueue:get_type(Q),
            Name = amqqueue:get_name(Q),
            #ctx{module = Mod,
                 name = Name,
                 state = Mod:init(Q)}
    end;
get_ctx(QPid, Contexts) when is_map(Contexts) ->
    Ref = qref(QPid),
    %% if we use a QPid it should always be initialised
    maps:get(Ref, Contexts, undefined).

set_ctx(Q, Ctx, Contexts)
  when ?is_amqqueue(Q) andalso is_map(Contexts) ->
    Ref = qref(Q),
    maps:put(Ref, Ctx, Contexts);
set_ctx(QPid, Ctx, Contexts) when is_map(Contexts) ->
    Ref = qref(QPid),
    maps:put(Ref, Ctx, Contexts).

qref(Pid) when is_pid(Pid) ->
    Pid;
qref(Q) when ?is_amqqueue(Q) ->
    qref(amqqueue:get_pid(Q));
qref({Name, _}) -> Name;
%% assume it already is a ref
qref(Ref) -> Ref.
