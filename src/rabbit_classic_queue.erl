-module(rabbit_classic_queue).
-behaviour(rabbit_queue_type).

-include("amqqueue.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-record(?MODULE, {pid :: pid()}).
-define(STATE, ?MODULE).

-opaque state() :: #?STATE{}.

-export_type([state/0]).

-export([
         is_enabled/0,
         declare/2,
         delete/4,
         stat/1,
         init/1,
         consume/3,
         cancel/6,
         handle_event/2,
         deliver/2,
         settle/4,
         reject/4,
         credit/4
         ]).

-export([delete_crashed/1,
         delete_crashed/2,
         delete_crashed_internal/2]).

is_enabled() -> true.

declare(Q, Node) when ?amqqueue_is_classic(Q) ->
    QName = amqqueue:get_name(Q),
    VHost = amqqueue:get_vhost(Q),
    Node1 = case rabbit_queue_master_location_misc:get_location(Q)  of
              {ok, Node0}  -> Node0;
              {error, _}   -> Node
            end,
    Node1 = rabbit_mirror_queue_misc:initial_queue_node(Q, Node1),
    case rabbit_vhost_sup_sup:get_vhost_sup(VHost, Node1) of
        {ok, _} ->
            gen_server2:call(
              rabbit_amqqueue_sup_sup:start_queue_process(Node1, Q, declare),
              {init, new}, infinity);
        {error, Error} ->
            rabbit_misc:protocol_error(internal_error,
                            "Cannot declare a queue '~s' on node '~s': ~255p",
                            [rabbit_misc:rs(QName), Node1, Error])
    end.

delete(Q, IfUnused, IfEmpty, ActingUser) when ?amqqueue_is_classic(Q) ->
    case wait_for_promoted_or_stopped(Q) of
        {promoted, Q1} ->
            QPid = amqqueue:get_pid(Q1),
            delegate:invoke(QPid, {gen_server2, call,
                                   [{delete, IfUnused, IfEmpty, ActingUser},
                                    infinity]});
        {stopped, Q1} ->
            #resource{name = Name, virtual_host = Vhost} = amqqueue:get_name(Q1),
            case IfEmpty of
                true ->
                    rabbit_log:error("Queue ~s in vhost ~s has its master node down and "
                                     "no mirrors available or eligible for promotion. "
                                     "The queue may be non-empty. "
                                     "Refusing to force-delete.",
                                     [Name, Vhost]),
                    {error, not_empty};
                false ->
                    rabbit_log:warning("Queue ~s in vhost ~s has its master node is down and "
                                       "no mirrors available or eligible for promotion. "
                                       "Forcing queue deletion.",
                                       [Name, Vhost]),
                    delete_crashed_internal(Q1, ActingUser),
                    {ok, 0}
            end;
        {error, not_found} ->
            %% Assume the queue was deleted
            {ok, 0}
    end.

stat(Q) ->
    delegate:invoke(amqqueue:get_pid(Q),
                    {gen_server2, call, [stat, infinity]}).

-spec init(amqqueue:amqqueue()) -> state().
init(Q) when ?amqqueue_is_classic(Q) ->
    #?STATE{pid = amqqueue:get_pid(Q)}.

consume(Q, Spec, State) when ?amqqueue_is_classic(Q) ->
    QPid = amqqueue:get_pid(Q),
    #{no_ack := NoAck,
      channel_pid := ChPid,
      limiter_pid := LimiterPid,
      limiter_active := LimiterActive,
      prefetch_count := ConsumerPrefetchCount,
      consumer_tag := ConsumerTag,
      exclusive_consume := ExclusiveConsume,
      args := Args,
      ok_msg := OkMsg,
      acting_user :=  ActingUser} = Spec,
    case delegate:invoke(QPid,
                         {gen_server2, call,
                          [{basic_consume, NoAck, ChPid, LimiterPid,
                            LimiterActive, ConsumerPrefetchCount, ConsumerTag,
                            ExclusiveConsume, Args, OkMsg, ActingUser},
                           infinity]}) of
        ok ->
            %% ask the host process to monitor this pid
            {ok, State, [{monitor, QPid, QPid}]};
        Err ->
            Err
    end.

cancel(Q, ChPid, ConsumerTag, OkMsg, ActingUser, State) ->
    QPid = amqqueue:get_pid(Q),
    case delegate:invoke(QPid, {gen_server2, call,
                                [{basic_cancel, ChPid, ConsumerTag,
                                  OkMsg, ActingUser}, infinity]}) of
        ok ->
            {ok, State};
        Err -> Err
    end.

-spec settle(rabbit_types:ctag(), [non_neg_integer()],
             ChPid :: pid(), state()) ->
    state().
settle(_CTag, MsgIds, ChPid, State) ->
    delegate:invoke_no_result(State#?STATE.pid,
                              {gen_server2, cast, [{ack, MsgIds, ChPid}]}),
    State.

reject(_CTag, Requeue, MsgIds, State) ->
    ChPid = self(),
    ok = delegate:invoke_no_result(State#?STATE.pid,
                                   {gen_server2, cast,
                                    [{reject, Requeue, MsgIds, ChPid}]}),
    State.

credit(CTag, Credit, Drain, State) ->
    ChPid = self(),
    delegate:invoke_no_result(State#?STATE.pid,
                              {gen_server2, cast,
                                     [{credit, ChPid, CTag, Credit, Drain}]}),
    State.

handle_event(_Evt, State) ->
    {ok, State, []}.

-spec deliver([{amqqueue:amqqueue(), state()}],
                  Delivery :: term()) ->
    {[{amqqueue:amqqueue(), state()}], rabbit_queue_type:actions()}.
deliver(Qs, #delivery{flow = Flow,
                      confirm = _Confirm} = Delivery) ->
    {MPids, SPids, Actions} = qpids(Qs),
    QPids = MPids ++ SPids,
    case Flow of
        %% Here we are tracking messages sent by the rabbit_channel
        %% process. We are accessing the rabbit_channel process
        %% dictionary.
        flow   -> [credit_flow:send(QPid) || QPid <- QPids],
                  [credit_flow:send(QPid) || QPid <- SPids];
        noflow -> ok
    end,
    MMsg = {deliver, Delivery, false},
    SMsg = {deliver, Delivery, true},
    rabbit_log:info("rabbit_classic_queue delivery confirm  ~w", [_Confirm, MMsg]),
    delegate:invoke_no_result(MPids, {gen_server2, cast, [MMsg]}),
    delegate:invoke_no_result(SPids, {gen_server2, cast, [SMsg]}),
    %% TODO: monitors
    {Qs, Actions}.

qpids(Qs) ->
    lists:foldl(fun ({Q, _}, {MPidAcc, SPidAcc, Actions0}) ->
                        QPid = amqqueue:get_pid(Q),
                        SPids = amqqueue:get_slave_pids(Q),
                        Actions = [{monitor, QPid, QPid}
                                   | [{monitor, P, QPid} || P <- SPids]] ++ Actions0,
                        {[QPid | MPidAcc], SPidAcc ++ SPids, Actions}
                end, {[], [], []}, Qs).
%% internal-ish
-spec wait_for_promoted_or_stopped(amqqueue:amqqueue()) ->
    {promoted, amqqueue:amqqueue()} |
    {stopped, amqqueue:amqqueue()} |
    {error, not_found}.
wait_for_promoted_or_stopped(Q0) ->
    QName = amqqueue:get_name(Q0),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            QPid = amqqueue:get_pid(Q),
            SPids = amqqueue:get_slave_pids(Q),
            case rabbit_mnesia:is_process_alive(QPid) of
                true  -> {promoted, Q};
                false ->
                    case lists:any(fun(Pid) ->
                                       rabbit_mnesia:is_process_alive(Pid)
                                   end, SPids) of
                        %% There is a live slave. May be promoted
                        true ->
                            timer:sleep(100),
                            wait_for_promoted_or_stopped(Q);
                        %% All slave pids are stopped.
                        %% No process left for the queue
                        false -> {stopped, Q}
                    end
            end;
        {error, not_found} ->
            {error, not_found}
    end.

-spec delete_crashed(amqqueue:amqqueue()) -> 'ok'.
delete_crashed(Q) ->
    delete_crashed(Q, ?INTERNAL_USER).

delete_crashed(Q, ActingUser) ->
    ok = rpc:call(amqqueue:qnode(Q), ?MODULE, delete_crashed_internal,
                  [Q, ActingUser]).

delete_crashed_internal(Q, ActingUser) ->
    QName = amqqueue:get_name(Q),
    {ok, BQ} = application:get_env(rabbit, backing_queue_module),
    BQ:delete_crashed(Q),
    ok = rabbit_amqqueue:internal_delete(QName, ActingUser).
