-module(rabbit_classic_queue).
-behaviour(rabbit_queue_type).

-include("amqqueue.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%% TODO possible to use sets / maps instead of lists?
%% Check performance with QoS 1 and 1 million target queues.
-record(msg_status, {pending :: [pid()],
                     confirmed = [] :: [pid()]}).

-define(STATE, ?MODULE).
-record(?STATE, {
           %% the current master pid
           pid :: undefined | pid(),
           unconfirmed = #{} :: #{non_neg_integer() => #msg_status{}},
           monitored = #{} :: #{pid() => ok}
          }).


-opaque state() :: #?STATE{}.

-export_type([state/0]).

-export([
         is_enabled/0,
         is_compatible/3,
         declare/2,
         delete/4,
         is_recoverable/1,
         recover/2,
         purge/1,
         policy_changed/1,
         stat/1,
         init/1,
         close/1,
         update/2,
         consume/3,
         cancel/5,
         handle_event/3,
         deliver/2,
         settle/5,
         credit/5,
         dequeue/5,
         info/2,
         state_info/1,
         capabilities/0,
         notify_decorators/1,
         is_stateful/0
         ]).

-export([delete_crashed/1,
         delete_crashed/2,
         delete_crashed_internal/2]).

-export([confirm_to_sender/3,
         send_rejection/3,
         deliver_to_consumer/5,
         send_drained/3,
         send_credit_reply/3]).

-spec is_enabled() -> boolean().
is_enabled() -> true.

-spec is_compatible(boolean(), boolean(), boolean()) -> boolean().
is_compatible(_, _, _) ->
    true.

declare(Q, Node) when ?amqqueue_is_classic(Q) ->
    QName = amqqueue:get_name(Q),
    VHost = amqqueue:get_vhost(Q),
    Node1 = case {Node, rabbit_amqqueue:is_exclusive(Q)} of
                {{ignore_location, Node0}, _} ->
                    Node0;
                {_, true} ->
                    Node;
                _ ->
                    case rabbit_queue_master_location_misc:get_location(Q) of
                        {ok, Node0}  -> Node0;
                        _   -> Node
                    end
            end,
    Node1 = rabbit_mirror_queue_misc:initial_queue_node(Q, Node1),
    case rabbit_vhost_sup_sup:get_vhost_sup(VHost, Node1) of
        {ok, _} ->
            gen_server2:call(
              rabbit_amqqueue_sup_sup:start_queue_process(Node1, Q, declare),
              {init, new}, infinity);
        {error, Error} ->
            {protocol_error, internal_error, "Cannot declare a queue '~ts' on node '~ts': ~255p",
             [rabbit_misc:rs(QName), Node1, Error]}
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
                    rabbit_log:error("Queue ~ts in vhost ~ts has its master node down and "
                                     "no mirrors available or eligible for promotion. "
                                     "The queue may be non-empty. "
                                     "Refusing to force-delete.",
                                     [Name, Vhost]),
                    {error, not_empty};
                false ->
                    rabbit_log:warning("Queue ~ts in vhost ~ts has its master node is down and "
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

is_recoverable(Q) when ?is_amqqueue(Q) ->
    Node = node(),
    Node =:= node(amqqueue:get_pid(Q)) andalso
    %% Terminations on node down will not remove the rabbit_queue
    %% record if it is a mirrored queue (such info is now obtained from
    %% the policy). Thus, we must check if the local pid is alive
    %% - if the record is present - in order to restart.
    (not rabbit_db_queue:consistent_exists(amqqueue:get_name(Q))
     orelse not rabbit_process:is_process_alive(amqqueue:get_pid(Q))).

recover(VHost, Queues) ->
    {ok, BQ} = application:get_env(rabbit, backing_queue_module),
    %% We rely on BQ:start/1 returning the recovery terms in the same
    %% order as the supplied queue names, so that we can zip them together
    %% for further processing in recover_durable_queues.
    {ok, OrderedRecoveryTerms} =
        BQ:start(VHost, [amqqueue:get_name(Q) || Q <- Queues]),
    case rabbit_amqqueue_sup_sup:start_for_vhost(VHost) of
        {ok, _}         ->
            RecoveredQs = recover_durable_queues(lists:zip(Queues,
                                                           OrderedRecoveryTerms)),
            FailedQs = find_missing_queues(Queues,RecoveredQs),
            {RecoveredQs, FailedQs};
        {error, Reason} ->
            rabbit_log:error("Failed to start queue supervisor for vhost '~ts': ~ts", [VHost, Reason]),
            throw({error, Reason})
    end.

find_missing_queues(Q1s, Q2s) when length(Q1s) == length(Q2s)->
    [];
find_missing_queues(Q1s, Q2s) ->
    find_missing_queues(lists:sort(Q1s), lists:sort(Q2s), []).

find_missing_queues([], _, Acc) ->
    Acc;
find_missing_queues(Q1s, [], Acc) ->
    Q1s ++ Acc;
find_missing_queues([Q1|Rem1], [Q2|Rem2] = Q2s, Acc) ->
    case amqqueue:get_name(Q1) == amqqueue:get_name(Q2) of
        true ->
            find_missing_queues(Rem1, Rem2, Acc);
        false ->
            find_missing_queues(Rem1, Q2s, [Q1|Acc])
    end.

-spec policy_changed(amqqueue:amqqueue()) -> ok.
policy_changed(Q) ->
    QPid = amqqueue:get_pid(Q),
    gen_server2:cast(QPid, policy_changed).

stat(Q) ->
    delegate:invoke(amqqueue:get_pid(Q),
                    {gen_server2, call, [stat, infinity]}).

-spec init(amqqueue:amqqueue()) -> {ok, state()}.
init(Q) when ?amqqueue_is_classic(Q) ->
    {ok, #?STATE{pid = amqqueue:get_pid(Q)}}.

-spec close(state()) -> ok.
close(_State) ->
    ok.

-spec update(amqqueue:amqqueue(), state()) -> state().
update(Q, #?STATE{pid = Pid} = State) when ?amqqueue_is_classic(Q) ->
    case amqqueue:get_pid(Q) of
        Pid ->
            State;
        NewPid ->
            %% master pid is different, update
            State#?STATE{pid = NewPid}
    end.

consume(Q, Spec, State0) when ?amqqueue_is_classic(Q) ->
    QPid = amqqueue:get_pid(Q),
    QRef = amqqueue:get_name(Q),
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
            %% TODO: track pids as they change
            State = ensure_monitor(QPid, QRef, State0),
            {ok, State#?STATE{pid = QPid}};
        Err ->
            Err
    end.

cancel(Q, ConsumerTag, OkMsg, ActingUser, State) ->
    QPid = amqqueue:get_pid(Q),
    case delegate:invoke(QPid, {gen_server2, call,
                                [{basic_cancel, self(), ConsumerTag,
                                  OkMsg, ActingUser}, infinity]}) of
        ok ->
            {ok, State};
        Err -> Err
    end.

-spec settle(rabbit_amqqueue:name(), rabbit_queue_type:settle_op(),
             rabbit_types:ctag(), [non_neg_integer()], state()) ->
    {state(), rabbit_queue_type:actions()}.
settle(_QName, complete, _CTag, MsgIds, State) ->
    Pid = State#?STATE.pid,
    delegate:invoke_no_result(Pid,
                              {gen_server2, cast, [{ack, MsgIds, self()}]}),
    {State, []};
settle(_QName, Op, _CTag, MsgIds, State) ->
    ChPid = self(),
    ok = delegate:invoke_no_result(State#?STATE.pid,
                                   {gen_server2, cast,
                                    [{reject, Op == requeue, MsgIds, ChPid}]}),
    {State, []}.

credit(_QName, CTag, Credit, Drain, State) ->
    ChPid = self(),
    delegate:invoke_no_result(State#?STATE.pid,
                              {gen_server2, cast,
                               [{credit, ChPid, CTag, Credit, Drain}]}),
    {State, []}.

handle_event(QName, {confirm, MsgSeqNos, Pid}, #?STATE{unconfirmed = U0} = State) ->
    %% confirms should never result in rejections
    {Unconfirmed, ConfirmedSeqNos, []} =
        settle_seq_nos(MsgSeqNos, Pid, U0, confirm),
    Actions = [{settled, QName, ConfirmedSeqNos}],
    %% handle confirm event from queues
    %% in this case the classic queue should track each individual publish and
    %% the processes involved and only emit a settle action once they have all
    %% been received (or DOWN has been received).
    %% Hence this part of the confirm logic is queue specific.
    {ok, State#?STATE{unconfirmed = Unconfirmed}, Actions};
handle_event(_QName, {deliver, _, _, _} = Delivery, #?STATE{} = State) ->
    {ok, State, [Delivery]};
handle_event(QName, {reject_publish, SeqNo, _QPid},
             #?STATE{unconfirmed = U0} = State) ->
    %% It does not matter which queue rejected the message,
    %% if any queue did, it should not be confirmed.
    {U, Rejected} = reject_seq_no(SeqNo, U0),
    Actions = [{rejected, QName, Rejected}],
    {ok, State#?STATE{unconfirmed = U}, Actions};
handle_event(QName, {down, Pid, Info}, #?STATE{monitored = Monitored,
                                               pid = MasterPid,
                                               unconfirmed = U0} = State0) ->
    State = State0#?STATE{monitored = maps:remove(Pid, Monitored)},
    Actions0 = case Pid =:= MasterPid of
                   true ->
                       [{queue_down, QName}];
                   false ->
                       []
              end,
    case rabbit_misc:is_abnormal_exit(Info) of
        false when Info =:= normal andalso Pid == MasterPid ->
            %% queue was deleted and masterpid is down
            {eol, []};
        false ->
            %% this assumes the mirror isn't part of the active set
            MsgSeqNos = maps:keys(
                          maps:filter(fun (_, #msg_status{pending = Pids}) ->
                                              lists:member(Pid, Pids)
                                      end, U0)),
            {Unconfirmed, Settled, Rejected} =
                settle_seq_nos(MsgSeqNos, Pid, U0, down),
            Actions = settlement_action(
                        settled, QName, Settled,
                        settlement_action(rejected, QName, Rejected, Actions0)),
            {ok, State#?STATE{unconfirmed = Unconfirmed}, Actions};
        true ->
            %% any abnormal exit should be considered a full reject of the
            %% oustanding message ids - If the message didn't get to all
            %% mirrors we have to assume it will never get there
            MsgIds = maps:fold(
                          fun (SeqNo, Status, Acc) ->
                                  case lists:member(Pid, Status#msg_status.pending) of
                                      true ->
                                          [SeqNo | Acc];
                                      false ->
                                          Acc
                                  end
                          end, [], U0),
            U = maps:without(MsgIds, U0),
            {ok, State#?STATE{unconfirmed = U},
             [{rejected, QName, MsgIds} | Actions0]}
    end;
handle_event(_QName, {send_drained, _} = Action, State) ->
    {ok, State, [Action]};
handle_event(_QName, {send_credit_reply, _} = Action, State) ->
    {ok, State, [Action]}.

settlement_action(_Type, _QRef, [], Acc) ->
    Acc;
settlement_action(Type, QRef, MsgSeqs, Acc) ->
    [{Type, QRef, MsgSeqs} | Acc].

-spec deliver([{amqqueue:amqqueue(), state()}],
              Delivery :: term()) ->
    {[{amqqueue:amqqueue(), state()}], rabbit_queue_type:actions()}.
deliver(Qs0, #delivery{flow = Flow,
                       msg_seq_no = MsgNo,
                       message = #basic_message{} = Msg0,
                       confirm = Confirm} = Delivery0) ->
    %% add guid to content here instead of in rabbit_basic:message/3,
    %% as classic queues are the only ones that need it
    Msg = Msg0#basic_message{id = rabbit_guid:gen()},
    Delivery = Delivery0#delivery{message = Msg},

    {MPids, SPids, Qs} = qpids(Qs0, Confirm, MsgNo),
    case Flow of
        %% Here we are tracking messages sent by the rabbit_channel
        %% process. We are accessing the rabbit_channel process
        %% dictionary.
        flow ->
            _ = [credit_flow:send(QPid) || QPid <- MPids],
            _ = [credit_flow:send(QPid) || QPid <- SPids],
            ok;
        noflow -> ok
    end,
    MMsg = {deliver, Delivery, false},
    SMsg = {deliver, Delivery, true},
    delegate:invoke_no_result(MPids, {gen_server2, cast, [MMsg]}),
    delegate:invoke_no_result(SPids, {gen_server2, cast, [SMsg]}),
    {Qs, []}.


-spec dequeue(rabbit_amqqueue:name(), NoAck :: boolean(),
              LimiterPid :: pid(), rabbit_types:ctag(), state()) ->
    {ok, Count :: non_neg_integer(), rabbit_amqqueue:qmsg(), state()} |
    {empty, state()}.
dequeue(_QName, NoAck, LimiterPid, _CTag, State) ->
    QPid = State#?STATE.pid,
    case delegate:invoke(QPid, {gen_server2, call,
                                [{basic_get, self(), NoAck, LimiterPid}, infinity]}) of
        empty ->
            {empty, State};
        {ok, Count, Msg} ->
            {ok, Count, Msg, State}
    end.

-spec state_info(state()) -> #{atom() := term()}.
state_info(_State) ->
    #{}.

%% general queue info
-spec info(amqqueue:amqqueue(), all_keys | rabbit_types:info_keys()) ->
    rabbit_types:infos().
info(Q, Items) ->
    QPid = amqqueue:get_pid(Q),
    Req = case Items of
              all_keys -> info;
              _ -> {info, Items}
          end,
    case delegate:invoke(QPid, {gen_server2, call, [Req, infinity]}) of
        {ok, Result} ->
            Result;
        {error, _Err} ->
            [];
        Result when is_list(Result) ->
            %% this is a backwards compatibility clause
            Result
    end.

-spec purge(amqqueue:amqqueue()) ->
    {ok, non_neg_integer()}.
purge(Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    delegate:invoke(QPid, {gen_server2, call, [purge, infinity]}).

qpids(Qs, Confirm, MsgNo) ->
    lists:foldl(
      fun ({Q, S0}, {MPidAcc, SPidAcc, Qs0}) ->
              QPid = amqqueue:get_pid(Q),
              SPids = amqqueue:get_slave_pids(Q),
              QRef = amqqueue:get_name(Q),
              S1 = ensure_monitor(QPid, QRef, S0),
              S2 = lists:foldl(fun(SPid, Acc) ->
                                       ensure_monitor(SPid, QRef, Acc)
                               end, S1, SPids),
              %% confirm record only if necessary
              S = case S2 of
                      #?STATE{unconfirmed = U0} ->
                          Rec = [QPid | SPids],
                          U = case Confirm of
                                  false ->
                                      U0;
                                  true ->
                                      U0#{MsgNo => #msg_status{pending = Rec}}
                              end,
                          S2#?STATE{pid = QPid,
                                    unconfirmed = U};
                      stateless ->
                          S2
                  end,
              {[QPid | MPidAcc], SPidAcc ++ SPids,
               [{Q, S} | Qs0]}
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
            case rabbit_process:is_process_alive(QPid) of
                true  -> {promoted, Q};
                false ->
                    case lists:any(fun(Pid) ->
                                       rabbit_process:is_process_alive(Pid)
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

-spec delete_crashed(amqqueue:amqqueue()) -> ok.
delete_crashed(Q) ->
    delete_crashed(Q, ?INTERNAL_USER).

delete_crashed(Q, ActingUser) ->
    ok = rpc:call(amqqueue:qnode(Q), ?MODULE, delete_crashed_internal,
                  [Q, ActingUser]).

delete_crashed_internal(Q, ActingUser) ->
    {ok, BQ} = application:get_env(rabbit, backing_queue_module),
    BQ:delete_crashed(Q),
    ok = rabbit_amqqueue:internal_delete(Q, ActingUser).

recover_durable_queues(QueuesAndRecoveryTerms) ->
    {Results, Failures} =
        gen_server2:mcall(
          [{rabbit_amqqueue_sup_sup:start_queue_process(node(), Q, recovery),
            {init, {self(), Terms}}} || {Q, Terms} <- QueuesAndRecoveryTerms]),
    [rabbit_log:error("Queue ~tp failed to initialise: ~tp",
                      [Pid, Error]) || {Pid, Error} <- Failures],
    [Q || {_, {new, Q}} <- Results].

capabilities() ->
    #{unsupported_policies => [%% Stream policies
                               <<"max-age">>, <<"stream-max-segment-size-bytes">>,
                               <<"queue-leader-locator">>, <<"initial-cluster-size">>,
                               %% Quorum policies
                               <<"delivery-limit">>, <<"dead-letter-strategy">>],
      queue_arguments => [<<"x-expires">>, <<"x-message-ttl">>, <<"x-dead-letter-exchange">>,
                          <<"x-dead-letter-routing-key">>, <<"x-max-length">>,
                          <<"x-max-length-bytes">>, <<"x-max-priority">>,
                          <<"x-overflow">>, <<"x-queue-mode">>, <<"x-queue-version">>,
                          <<"x-single-active-consumer">>, <<"x-queue-type">>,
                          <<"x-queue-master-locator">>],
      consumer_arguments => [<<"x-cancel-on-ha-failover">>,
                             <<"x-priority">>, <<"x-credit">>],
      server_named => true}.

notify_decorators(Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    delegate:invoke_no_result(QPid, {gen_server2, cast, [notify_decorators]}).

is_stateful() -> true.

reject_seq_no(SeqNo, U0) ->
    reject_seq_no(SeqNo, U0, []).

reject_seq_no(SeqNo, U0, Acc) ->
    case maps:take(SeqNo, U0) of
        {_, U} ->
            {U, [SeqNo | Acc]};
        error ->
            {U0, Acc}
    end.

settle_seq_nos(MsgSeqNos, Pid, U0, Reason) ->
    lists:foldl(
      fun (SeqNo, {U, C0, R0}) ->
              case U of
                  #{SeqNo := Status0} ->
                      case update_msg_status(Reason, Pid, Status0) of
                          #msg_status{pending = [],
                                      confirmed = []} ->
                              %% no pending left and nothing confirmed
                              %% then we reject it
                              {maps:remove(SeqNo, U), C0, [SeqNo | R0]};
                          #msg_status{pending = [],
                                      confirmed = _} ->
                              %% this can be confirmed as there are no pending
                              %% and confirmed isn't empty
                              {maps:remove(SeqNo, U), [SeqNo | C0], R0};
                          MsgStatus ->
                              {U#{SeqNo => MsgStatus}, C0, R0}
                      end;
                  _ ->
                      {U, C0, R0}
              end
      end, {U0, [], []}, MsgSeqNos).

update_msg_status(confirm, Pid, #msg_status{pending = P,
                                            confirmed = C} = S) ->
    Rem = lists:delete(Pid, P),
    S#msg_status{pending = Rem, confirmed = [Pid | C]};
update_msg_status(down, Pid, #msg_status{pending = P} = S) ->
    S#msg_status{pending = lists:delete(Pid, P)}.

ensure_monitor(_, _, State = stateless) ->
    State;
ensure_monitor(Pid, _, State = #?STATE{monitored = Monitored})
  when is_map_key(Pid, Monitored) ->
    State;
ensure_monitor(Pid, QName, State = #?STATE{monitored = Monitored}) ->
    _ = erlang:monitor(process, Pid, [{tag, {'DOWN', QName}}]),
    State#?STATE{monitored = Monitored#{Pid => ok}}.

%% part of channel <-> queue api
confirm_to_sender(Pid, QName, MsgSeqNos) ->
    Msg = {confirm, MsgSeqNos, self()},
    gen_server:cast(Pid, {queue_event, QName, Msg}).

send_rejection(Pid, QName, MsgSeqNo) ->
    Msg = {reject_publish, MsgSeqNo, self()},
    gen_server:cast(Pid, {queue_event, QName, Msg}).

deliver_to_consumer(Pid, QName, CTag, AckRequired, Message) ->
    Deliver = {deliver, CTag, AckRequired, [Message]},
    Evt = {queue_event, QName, Deliver},
    gen_server:cast(Pid, Evt).

send_drained(Pid, QName, CTagCredits) when is_list(CTagCredits) ->
    [_ = gen_server:cast(Pid, {queue_event, QName,
                               {send_drained, CTagCredit}})
     || CTagCredit <- CTagCredits],
    ok;
send_drained(Pid, QName, CTagCredit) when is_tuple(CTagCredit) ->
    gen_server:cast(Pid, {queue_event, QName,
                          {send_drained, CTagCredit}}).

send_credit_reply(Pid, QName, Len) when is_integer(Len) ->
    gen_server:cast(Pid, {queue_event, QName,
                          {send_credit_reply, Len}}).
