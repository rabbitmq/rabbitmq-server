-module(rabbit_classic_queue).
-behaviour(rabbit_queue_type).
-behaviour(rabbit_policy_validator).

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
         format/2,
         init/1,
         close/1,
         update/2,
         consume/3,
         cancel/3,
         handle_event/3,
         deliver/3,
         settle/5,
         credit_v1/5,
         credit/6,
         dequeue/5,
         info/2,
         state_info/1,
         capabilities/0,
         notify_decorators/1,
         is_stateful/0
         ]).

-export([delete_crashed/1,
         delete_crashed/2,
         delete_crashed_internal/2,
         delete_crashed_in_backing_queue/1]).

-export([confirm_to_sender/3,
         send_rejection/3,
         deliver_to_consumer/5,
         send_credit_reply_credit_api_v1/3,
         send_drained_credit_api_v1/4,
         send_credit_reply/7]).

-export([policy_apply_to_name/0,
         stop/1,
         list_with_minimum_quorum/0,
         drain/1,
         revive/0,
         queue_vm_stats_sups/0,
         queue_vm_ets/0]).

-export([validate_policy/1]).

-rabbit_boot_step(
   {rabbit_classic_queue_type,
    [{description, "Classic queue: queue type"},
     {mfa,      {rabbit_registry, register,
                    [queue, <<"classic">>, ?MODULE]}},
     {cleanup,  {rabbit_registry, unregister,
                 [queue, <<"classic">>]}},
     {requires, rabbit_registry},
     {enables,     ?MODULE}]}).

-rabbit_boot_step(
   {?MODULE,
    [{description, "Deprecated queue-master-locator support."
      "Use queue-leader-locator instead."},
     {mfa, {rabbit_registry, register,
            [policy_validator, <<"queue-master-locator">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [operator_policy_validator, <<"queue-master-locator">>, ?MODULE]}},
     {requires, [rabbit_classic_queue_type]},
     {enables, recovery}]}).

validate_policy(Args) ->
    %% queue-master-locator was deprecated in 4.0
    Locator = proplists:get_value(<<"queue-master-locator">>, Args, unknown),
    case Locator of
        unknown ->
            ok;
        _ ->
            case rabbit_queue_location:master_locator_permitted() of
                true ->
                    case lists:member(Locator, rabbit_queue_location:queue_leader_locators()) of
                        true -> ok;
                        false -> {error, "~tp is not a valid master locator", [Locator]}
                    end;
                false ->
                    {error, "use of deprecated queue-master-locator argument is not permitted", []}
                end
    end.

-spec is_enabled() -> boolean().
is_enabled() -> true.

-spec is_compatible(boolean(), boolean(), boolean()) -> boolean().
is_compatible(_, _, _) ->
    true.

validate_arguments(Args) ->
    case lists:keymember(<<"x-queue-master-locator">>, 1, Args) of
        false ->
            ok;
        true ->
            case rabbit_queue_location:master_locator_permitted() of
                true ->
                    ok;
                false ->
                    Warning = rabbit_deprecated_features:get_warning(
                                queue_master_locator),
                    {protocol_error, internal_error, "~ts", [Warning]}
            end
    end.

declare(Q, Node) when ?amqqueue_is_classic(Q) ->
    case validate_arguments(amqqueue:get_arguments(Q)) of
        ok -> do_declare(Q, Node);
        Error -> Error
    end.

do_declare(Q, Node) when ?amqqueue_is_classic(Q) ->
    QName = amqqueue:get_name(Q),
    VHost = amqqueue:get_vhost(Q),
    Node1 = case {Node, rabbit_amqqueue:is_exclusive(Q)} of
                {{ignore_location, Node0}, _} ->
                    Node0;
                {_, true} ->
                    Node;
                _ ->
                    {Node0, _} = rabbit_queue_location:select_leader_and_followers(Q, 1),
                    Node0
            end,
    case rabbit_vhost_sup_sup:get_vhost_sup(VHost, Node1) of
        {ok, _} ->
            gen_server2:call(
              rabbit_amqqueue_sup_sup:start_queue_process(Node1, Q),
              {init, new}, infinity);
        {error, Error} ->
            {protocol_error, internal_error, "Cannot declare a queue '~ts' on node '~ts': ~255p",
             [rabbit_misc:rs(QName), Node1, Error]}
    end.

delete(Q0, IfUnused, IfEmpty, ActingUser) when ?amqqueue_is_classic(Q0) ->
    QName = amqqueue:get_name(Q0),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            QPid = amqqueue:get_pid(Q),
            case rabbit_process:is_process_alive(QPid) of
                true ->
                    delegate:invoke(QPid, {gen_server2, call,
                                           [{delete, IfUnused, IfEmpty, ActingUser},
                                            infinity]});
                false ->
                    #resource{name = Name, virtual_host = Vhost} = QName,
                    case IfEmpty of
                        true ->
                            rabbit_log:error("Queue ~ts in vhost ~ts is down. "
                                             "The queue may be non-empty. "
                                             "Refusing to force-delete.",
                                             [Name, Vhost]),
                            {error, not_empty};
                        false ->
                            rabbit_log:warning("Queue ~ts in vhost ~ts is down. "
                                               "Forcing queue deletion.",
                                               [Name, Vhost]),
                            case delete_crashed_internal(Q, ActingUser) of
                                ok ->
                                    {ok, 0};
                                {error, timeout} = Err ->
                                    Err
                            end
                    end
            end;
        {error, not_found} ->
            %% Assume the queue was deleted
            {ok, 0}
    end.

is_recoverable(Q) when ?is_amqqueue(Q) and ?amqqueue_is_classic(Q) ->
    Node = node(),
    Node =:= amqqueue:qnode(Q) andalso
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
    case rabbit_khepri:is_enabled() of
        false ->
            gen_server2:cast(QPid, policy_changed);
        true ->
            %% When using Khepri, projections are guaranteed to be atomic on
            %% the node that processes them, but there might be a slight delay
            %% until they're applied on other nodes. Some test suites fail
            %% intermittently, showing that rabbit_amqqueue_process is reading
            %% the old policy value. We use the khepri ff to hide this API change,
            %% and use the up-to-date record to update the policy on the gen_server
            %% state.
            gen_server2:cast(QPid, {policy_changed, Q})
    end.

stat(Q) ->
    delegate:invoke(amqqueue:get_pid(Q),
                    {gen_server2, call, [stat, infinity]}).


format(Q, _Ctx) when ?is_amqqueue(Q) ->
    State = case amqqueue:get_state(Q) of
                live ->
                    running;
                S ->
                    S
            end,
    [{type, rabbit_queue_type:short_alias_of(?MODULE)},
     {state, State},
     {node, node(amqqueue:get_pid(Q))}].

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
      mode := Mode,
      consumer_tag := ConsumerTag,
      exclusive_consume := ExclusiveConsume,
      args := Args0,
      ok_msg := OkMsg,
      acting_user :=  ActingUser} = Spec,
    {ModeOrPrefetch, Args} = consume_backwards_compat(Mode, Args0),
    case delegate:invoke(QPid,
                         {gen_server2, call,
                          [{basic_consume, NoAck, ChPid, LimiterPid,
                            LimiterActive, ModeOrPrefetch, ConsumerTag,
                            ExclusiveConsume, Args, OkMsg, ActingUser},
                           infinity]}) of
        ok ->
            %% TODO: track pids as they change
            State = ensure_monitor(QPid, QRef, State0),
            {ok, State#?STATE{pid = QPid}};
        {error, exclusive_consume_unavailable} ->
            {error, access_refused, "~ts in exclusive use",
             [rabbit_misc:rs(QRef)]};
        {error, Reason} ->
            {error, internal_error, "failed consuming from classic ~ts: ~tp",
             [rabbit_misc:rs(QRef), Reason]}
    end.

%% Delete this function when feature flag rabbitmq_4.0.0 becomes required.
consume_backwards_compat({simple_prefetch, PrefetchCount} = Mode, Args) ->
    case rabbit_feature_flags:is_enabled('rabbitmq_4.0.0') of
        true -> {Mode, Args};
        false -> {PrefetchCount, Args}
    end;
consume_backwards_compat({credited, InitialDeliveryCount} = Mode, Args)
  when is_integer(InitialDeliveryCount) ->
    %% credit API v2
    {Mode, Args};
consume_backwards_compat({credited, credit_api_v1}, Args) ->
    %% credit API v1
    {_PrefetchCount = 0,
     [{<<"x-credit">>, table, [{<<"credit">>, long, 0},
                               {<<"drain">>,  bool, false}]} | Args]}.

cancel(Q, Spec, State) ->
    %% Cancel API v2 reuses feature flag rabbitmq_4.0.0.
    Request = case rabbit_feature_flags:is_enabled('rabbitmq_4.0.0') of
                  true ->
                      {stop_consumer, Spec#{pid => self()}};
                  false ->
                      #{consumer_tag := ConsumerTag,
                        user := ActingUser} = Spec,
                      OkMsg = maps:get(ok_msg, Spec, undefined),
                      {basic_cancel, self(), ConsumerTag, OkMsg, ActingUser}
              end,
    case delegate:invoke(amqqueue:get_pid(Q),
                         {gen_server2, call, [Request, infinity]}) of
        ok -> {ok, State};
        Err -> Err
    end.

-spec settle(rabbit_amqqueue:name(), rabbit_queue_type:settle_op(),
             rabbit_types:ctag(), [non_neg_integer()], state()) ->
    {state(), rabbit_queue_type:actions()}.
settle(QName, {modify, _DelFailed, Undel, _Anns}, CTag, MsgIds, State) ->
    %% translate modify into other op
    Op = case Undel of
             true ->
                 discard;
             false ->
                 requeue
         end,
    settle(QName, Op, CTag, MsgIds, State);
settle(_QName, Op, _CTag, MsgIds, State = #?STATE{pid = Pid}) ->
    Arg = case Op of
              complete ->
                  {ack, MsgIds, self()};
              _ ->
                  {reject, Op == requeue, MsgIds, self()}
          end,
    delegate:invoke_no_result(Pid, {gen_server2, cast, [Arg]}),
    {State, []}.

credit_v1(_QName, Ctag, LinkCreditSnd, Drain, #?STATE{pid = QPid} = State) ->
    Request = {credit, self(), Ctag, LinkCreditSnd, Drain},
    delegate:invoke_no_result(QPid, {gen_server2, cast, [Request]}),
    {State, []}.

credit(_QName, Ctag, DeliveryCountRcv, LinkCreditRcv, Drain, #?STATE{pid = QPid} = State) ->
    Request = {credit, self(), Ctag, DeliveryCountRcv, LinkCreditRcv, Drain},
    delegate:invoke_no_result(QPid, {gen_server2, cast, [Request]}),
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
                                               unconfirmed = U0} = State0) ->
    State = State0#?STATE{monitored = maps:remove(Pid, Monitored)},
    Actions0 = [{queue_down, QName}],
    case rabbit_misc:is_abnormal_exit(Info) of
        false when Info =:= normal ->
            %% queue was deleted
            {eol, []};
        false ->
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
            %% oustanding message ids
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
handle_event(_QName, Action, State)
  when element(1, Action) =:= credit_reply ->
    {ok, State, [Action]};
handle_event(_QName, {send_drained, {Ctag, Credit}}, State) ->
    %% This function clause should be deleted when feature flag
    %% rabbitmq_4.0.0 becomes required.
    Action = {credit_reply_v1, Ctag, Credit, _Available = 0, _Drain = true},
    {ok, State, [Action]}.

settlement_action(_Type, _QRef, [], Acc) ->
    Acc;
settlement_action(Type, QRef, MsgSeqs, Acc) ->
    [{Type, QRef, MsgSeqs} | Acc].

-spec deliver([{amqqueue:amqqueue(), state()}],
              Delivery :: mc:state(),
              rabbit_queue_type:delivery_options()) ->
    {[{amqqueue:amqqueue(), state()}], rabbit_queue_type:actions()}.
deliver(Qs0, Msg0, Options) ->
    %% add guid to content here instead of in rabbit_basic:message/3,
    %% as classic queues are the only ones that need it
    Msg = mc:prepare(store, mc:set_annotation(id, rabbit_guid:gen(), Msg0)),
    Mandatory = maps:get(mandatory, Options, false),
    MsgSeqNo = maps:get(correlation, Options, undefined),
    Flow = maps:get(flow, Options, noflow),
    Confirm = MsgSeqNo /= undefined,

    {MPids, Qs} = qpids(Qs0, Confirm, MsgSeqNo),
    Delivery = rabbit_basic:delivery(Mandatory, Confirm, Msg, MsgSeqNo, Flow),

    case Flow of
        %% Here we are tracking messages sent by the rabbit_channel
        %% process. We are accessing the rabbit_channel process
        %% dictionary.
        flow ->
            _ = [credit_flow:send(QPid) || QPid <- MPids],
            ok;
        noflow -> ok
    end,
    MMsg = {deliver, Delivery, false},
    delegate:invoke_no_result(MPids, {gen_server2, cast, [MMsg]}),
    {Qs, []}.

-spec dequeue(rabbit_amqqueue:name(), NoAck :: boolean(),
              LimiterPid :: pid(), rabbit_types:ctag(), state()) ->
    {ok, Count :: non_neg_integer(), rabbit_amqqueue:qmsg(), state()} |
    {empty, state()}.
dequeue(QName, NoAck, LimiterPid, _CTag, State0) ->
    QPid = State0#?STATE.pid,
    State1 = ensure_monitor(QPid, QName, State0),
    case delegate:invoke(QPid, {gen_server2, call,
                                [{basic_get, self(), NoAck, LimiterPid}, infinity]}) of
        empty ->
            {empty, State1};
        {ok, Count, Msg} ->
            {ok, Count, Msg, State1}
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
      fun ({Q, S0}, {MPidAcc, Qs0}) ->
              QPid = amqqueue:get_pid(Q),
              QRef = amqqueue:get_name(Q),
              S1 = ensure_monitor(QPid, QRef, S0),
              %% confirm record only if necessary
              S = case S1 of
                      #?STATE{unconfirmed = U0} ->
                          Rec = [QPid],
                          U = case Confirm of
                                  false ->
                                      U0;
                                  true ->
                                      U0#{MsgNo => #msg_status{pending = Rec}}
                              end,
                          S1#?STATE{pid = QPid,
                                    unconfirmed = U};
                      stateless ->
                          S1
                  end,
              {[QPid | MPidAcc], [{Q, S} | Qs0]}
      end, {[], []}, Qs).

-spec delete_crashed(amqqueue:amqqueue()) -> ok.
delete_crashed(Q) ->
    delete_crashed(Q, ?INTERNAL_USER).

delete_crashed(Q, ActingUser) ->
    %% Delete from `rabbit_db_queue' from the queue's node. The deletion's
    %% change to the Khepri projection is immediately consistent on that node,
    %% so the call will block until that node has fully deleted and forgotten
    %% about the queue.
    Ret = rpc:call(amqqueue:qnode(Q), ?MODULE, delete_crashed_in_backing_queue,
                   [Q]),
    case Ret of
        {badrpc, {'EXIT', {undef, _}}} ->
            %% Compatibility: if the remote node doesn't yet expose this
            %% function, call it directly on this node.
            ok = delete_crashed_in_backing_queue(Q);
        ok ->
            ok
    end,
    ok = rabbit_amqqueue:internal_delete(Q, ActingUser).

delete_crashed_internal(Q, ActingUser) ->
    delete_crashed_in_backing_queue(Q),
    rabbit_amqqueue:internal_delete(Q, ActingUser).

delete_crashed_in_backing_queue(Q) ->
    {ok, BQ} = application:get_env(rabbit, backing_queue_module),
    BQ:delete_crashed(Q).

recover_durable_queues(QueuesAndRecoveryTerms) ->
    {Results, Failures} =
        gen_server2:mcall(
          [{rabbit_amqqueue_sup_sup:start_queue_process(node(), Q),
            {init, {self(), Terms}}} || {Q, Terms} <- QueuesAndRecoveryTerms]),
    [rabbit_log:error("Queue ~tp failed to initialise: ~tp",
                      [Pid, Error]) || {Pid, Error} <- Failures],
    [Q || {_, {new, Q}} <- Results].

capabilities() ->
    #{unsupported_policies => [%% Stream policies
                               <<"max-age">>, <<"stream-max-segment-size-bytes">>,
                               <<"initial-cluster-size">>,
                               %% Quorum policies
                               <<"delivery-limit">>, <<"dead-letter-strategy">>,
                               <<"max-in-memory-length">>, <<"max-in-memory-bytes">>,
                               <<"target-group-size">>, <<"filter-field-names">>],
      queue_arguments => [<<"x-expires">>, <<"x-message-ttl">>, <<"x-dead-letter-exchange">>,
                          <<"x-dead-letter-routing-key">>, <<"x-max-length">>,
                          <<"x-max-length-bytes">>, <<"x-max-priority">>,
                          <<"x-overflow">>, <<"x-queue-mode">>, <<"x-queue-version">>,
                          <<"x-single-active-consumer">>, <<"x-queue-type">>, <<"x-queue-master-locator">>]
                          ++ case rabbit_feature_flags:is_enabled('rabbitmq_4.0.0') of
                                 true -> [<<"x-queue-leader-locator">>];
                                 false -> []
                             end,
      consumer_arguments => [<<"x-priority">>],
      server_named => true,
      rebalance_module => undefined,
      can_redeliver => false,
      is_replicable => false
     }.

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
    Evt = {confirm, MsgSeqNos, self()},
    send_queue_event(Pid, QName, Evt).

send_rejection(Pid, QName, MsgSeqNo) ->
    Evt = {reject_publish, MsgSeqNo, self()},
    send_queue_event(Pid, QName, Evt).

deliver_to_consumer(Pid, QName, CTag, AckRequired, Message) ->
    Evt = {deliver, CTag, AckRequired, [Message]},
    send_queue_event(Pid, QName, Evt).

%% Delete this function when feature flag rabbitmq_4.0.0 becomes required.
send_credit_reply_credit_api_v1(Pid, QName, Available) ->
    Evt = {send_credit_reply, Available},
    send_queue_event(Pid, QName, Evt).

%% Delete this function when feature flag rabbitmq_4.0.0 becomes required.
send_drained_credit_api_v1(Pid, QName, Ctag, Credit) ->
    Evt = {send_drained, {Ctag, Credit}},
    send_queue_event(Pid, QName, Evt).

send_credit_reply(Pid, QName, Ctag, DeliveryCount, Credit, Available, Drain) ->
    Evt = {credit_reply, Ctag, DeliveryCount, Credit, Available, Drain},
    send_queue_event(Pid, QName, Evt).

send_queue_event(Pid, QName, Event) ->
    gen_server:cast(Pid, {queue_event, QName, Event}).

policy_apply_to_name() ->
    <<"classic_queues">>.

stop(VHost) ->
    ok = rabbit_amqqueue_sup_sup:stop_for_vhost(VHost),
    {ok, BQ} = application:get_env(rabbit, backing_queue_module),
    ok = BQ:stop(VHost).

list_with_minimum_quorum() ->
    [].

drain(_TransferCandidates) ->
    ok.

revive() ->
    ok.

queue_vm_stats_sups() ->
    {[queue_procs], [rabbit_vm:all_vhosts_children(rabbit_amqqueue_sup_sup)]}.

%% return nothing because of this line in rabbit_vm:
%% {msg_index,            MsgIndexETS + MsgIndexProc},
%% it mixes procs and ets,
%% TODO: maybe instead of separating sups and ets
%% I need vm_memory callback that just
%% returns proplist? And rabbit_vm calculates
%% Other as usual by substraction.
queue_vm_ets() ->
    {[], []}.
