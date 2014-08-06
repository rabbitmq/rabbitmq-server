%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_amqqueue).

-export([recover/0, stop/0, start/1, declare/5, declare/6,
         delete_immediately/1, delete/3, purge/1, forget_all_durable/1]).
-export([pseudo_queue/2, immutable/1]).
-export([lookup/1, not_found_or_absent/1, with/2, with/3, with_or_die/2,
         assert_equivalence/5,
         check_exclusive_access/2, with_exclusive_access_or_die/3,
         stat/1, deliver/2, deliver_flow/2, requeue/3, ack/3, reject/4]).
-export([list/0, list/1, info_keys/0, info/1, info/2, info_all/1, info_all/2]).
-export([force_event_refresh/1, notify_policy_changed/1]).
-export([consumers/1, consumers_all/1, consumer_info_keys/0]).
-export([basic_get/4, basic_consume/10, basic_cancel/4, notify_decorators/1]).
-export([notify_sent/2, notify_sent_queue_down/1, resume/2]).
-export([notify_down_all/2, activate_limit_all/2, credit/5]).
-export([on_node_up/1, on_node_down/1]).
-export([update/2, store_queue/1, update_decorators/1, policy_changed/2]).
-export([start_mirroring/1, stop_mirroring/1, sync_mirrors/1,
         cancel_sync_mirrors/1]).

%% internal
-export([internal_declare/2, internal_delete/1, run_backing_queue/3,
         set_ram_duration_target/2, set_maximum_since_use/2]).

-include("rabbit.hrl").
-include_lib("stdlib/include/qlc.hrl").

-define(INTEGER_ARG_TYPES, [byte, short, signedint, long]).

-define(MORE_CONSUMER_CREDIT_AFTER, 50).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([name/0, qmsg/0]).

-type(name() :: rabbit_types:r('queue')).
-type(qpids() :: [pid()]).
-type(qlen() :: rabbit_types:ok(non_neg_integer())).
-type(qfun(A) :: fun ((rabbit_types:amqqueue()) -> A | no_return())).
-type(qmsg() :: {name(), pid(), msg_id(), boolean(), rabbit_types:message()}).
-type(msg_id() :: non_neg_integer()).
-type(ok_or_errors() ::
        'ok' | {'error', [{'error' | 'exit' | 'throw', any()}]}).
-type(queue_or_absent() :: rabbit_types:amqqueue() |
                           {'absent', rabbit_types:amqqueue()}).
-type(not_found_or_absent() :: 'not_found' |
                               {'absent', rabbit_types:amqqueue()}).
-spec(recover/0 :: () -> [rabbit_types:amqqueue()]).
-spec(stop/0 :: () -> 'ok').
-spec(start/1 :: ([rabbit_types:amqqueue()]) -> 'ok').
-spec(declare/5 ::
        (name(), boolean(), boolean(),
         rabbit_framing:amqp_table(), rabbit_types:maybe(pid()))
        -> {'new' | 'existing' | 'absent' | 'owner_died',
            rabbit_types:amqqueue()} | rabbit_types:channel_exit()).
-spec(declare/6 ::
        (name(), boolean(), boolean(),
         rabbit_framing:amqp_table(), rabbit_types:maybe(pid()), node())
        -> {'new' | 'existing' | 'absent' | 'owner_died',
            rabbit_types:amqqueue()} | rabbit_types:channel_exit()).
-spec(internal_declare/2 ::
        (rabbit_types:amqqueue(), boolean())
        -> queue_or_absent() | rabbit_misc:thunk(queue_or_absent())).
-spec(update/2 ::
        (name(),
         fun((rabbit_types:amqqueue()) -> rabbit_types:amqqueue()))
         -> 'not_found' | rabbit_types:amqqueue()).
-spec(lookup/1 ::
        (name()) -> rabbit_types:ok(rabbit_types:amqqueue()) |
                    rabbit_types:error('not_found');
        ([name()]) -> [rabbit_types:amqqueue()]).
-spec(not_found_or_absent/1 :: (name()) -> not_found_or_absent()).
-spec(with/2 :: (name(), qfun(A)) ->
                     A | rabbit_types:error(not_found_or_absent())).
-spec(with/3 :: (name(), qfun(A), fun((not_found_or_absent()) -> B)) -> A | B).
-spec(with_or_die/2 ::
        (name(), qfun(A)) -> A | rabbit_types:channel_exit()).
-spec(assert_equivalence/5 ::
        (rabbit_types:amqqueue(), boolean(), boolean(),
         rabbit_framing:amqp_table(), rabbit_types:maybe(pid()))
        -> 'ok' | rabbit_types:channel_exit() |
           rabbit_types:connection_exit()).
-spec(check_exclusive_access/2 ::
        (rabbit_types:amqqueue(), pid())
        -> 'ok' | rabbit_types:channel_exit()).
-spec(with_exclusive_access_or_die/3 ::
        (name(), pid(), qfun(A)) -> A | rabbit_types:channel_exit()).
-spec(list/0 :: () -> [rabbit_types:amqqueue()]).
-spec(list/1 :: (rabbit_types:vhost()) -> [rabbit_types:amqqueue()]).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(info/1 :: (rabbit_types:amqqueue()) -> rabbit_types:infos()).
-spec(info/2 ::
        (rabbit_types:amqqueue(), rabbit_types:info_keys())
        -> rabbit_types:infos()).
-spec(info_all/1 :: (rabbit_types:vhost()) -> [rabbit_types:infos()]).
-spec(info_all/2 :: (rabbit_types:vhost(), rabbit_types:info_keys())
                    -> [rabbit_types:infos()]).
-spec(force_event_refresh/1 :: (reference()) -> 'ok').
-spec(notify_policy_changed/1 :: (rabbit_types:amqqueue()) -> 'ok').
-spec(consumers/1 :: (rabbit_types:amqqueue())
                     -> [{pid(), rabbit_types:ctag(), boolean(),
                          non_neg_integer(), rabbit_framing:amqp_table()}]).
-spec(consumer_info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(consumers_all/1 ::
        (rabbit_types:vhost())
        -> [{name(), pid(), rabbit_types:ctag(), boolean(),
             non_neg_integer(), rabbit_framing:amqp_table()}]).
-spec(stat/1 ::
        (rabbit_types:amqqueue())
        -> {'ok', non_neg_integer(), non_neg_integer()}).
-spec(delete_immediately/1 :: (qpids()) -> 'ok').
-spec(delete/3 ::
        (rabbit_types:amqqueue(), 'false', 'false')
        -> qlen();
        (rabbit_types:amqqueue(), 'true' , 'false')
        -> qlen() | rabbit_types:error('in_use');
        (rabbit_types:amqqueue(), 'false', 'true' )
        -> qlen() | rabbit_types:error('not_empty');
        (rabbit_types:amqqueue(), 'true' , 'true' )
        -> qlen() |
           rabbit_types:error('in_use') |
           rabbit_types:error('not_empty')).
-spec(purge/1 :: (rabbit_types:amqqueue()) -> qlen()).
-spec(forget_all_durable/1 :: (node()) -> 'ok').
-spec(deliver/2 :: ([rabbit_types:amqqueue()], rabbit_types:delivery()) ->
                        qpids()).
-spec(deliver_flow/2 :: ([rabbit_types:amqqueue()], rabbit_types:delivery()) ->
                             qpids()).
-spec(requeue/3 :: (pid(), [msg_id()],  pid()) -> 'ok').
-spec(ack/3 :: (pid(), [msg_id()], pid()) -> 'ok').
-spec(reject/4 :: (pid(), [msg_id()], boolean(), pid()) -> 'ok').
-spec(notify_down_all/2 :: (qpids(), pid()) -> ok_or_errors()).
-spec(activate_limit_all/2 :: (qpids(), pid()) -> ok_or_errors()).
-spec(basic_get/4 :: (rabbit_types:amqqueue(), pid(), boolean(), pid()) ->
                          {'ok', non_neg_integer(), qmsg()} | 'empty').
-spec(credit/5 :: (rabbit_types:amqqueue(), pid(), rabbit_types:ctag(),
                   non_neg_integer(), boolean()) -> 'ok').
-spec(basic_consume/10 ::
        (rabbit_types:amqqueue(), boolean(), pid(), pid(), boolean(),
         non_neg_integer(), rabbit_types:ctag(), boolean(),
         rabbit_framing:amqp_table(), any())
        -> rabbit_types:ok_or_error('exclusive_consume_unavailable')).
-spec(basic_cancel/4 ::
        (rabbit_types:amqqueue(), pid(), rabbit_types:ctag(), any()) -> 'ok').
-spec(notify_decorators/1 :: (rabbit_types:amqqueue()) -> 'ok').
-spec(notify_sent/2 :: (pid(), pid()) -> 'ok').
-spec(notify_sent_queue_down/1 :: (pid()) -> 'ok').
-spec(resume/2 :: (pid(), pid()) -> 'ok').
-spec(internal_delete/1 ::
        (name()) -> rabbit_types:ok_or_error('not_found') |
                    rabbit_types:connection_exit() |
                    fun (() -> rabbit_types:ok_or_error('not_found') |
                               rabbit_types:connection_exit())).
-spec(run_backing_queue/3 ::
        (pid(), atom(),
         (fun ((atom(), A) -> {[rabbit_types:msg_id()], A}))) -> 'ok').
-spec(set_ram_duration_target/2 :: (pid(), number() | 'infinity') -> 'ok').
-spec(set_maximum_since_use/2 :: (pid(), non_neg_integer()) -> 'ok').
-spec(on_node_up/1 :: (node()) -> 'ok').
-spec(on_node_down/1 :: (node()) -> 'ok').
-spec(pseudo_queue/2 :: (name(), pid()) -> rabbit_types:amqqueue()).
-spec(immutable/1 :: (rabbit_types:amqqueue()) -> rabbit_types:amqqueue()).
-spec(store_queue/1 :: (rabbit_types:amqqueue()) -> 'ok').
-spec(update_decorators/1 :: (name()) -> 'ok').
-spec(policy_changed/2 ::
        (rabbit_types:amqqueue(), rabbit_types:amqqueue()) -> 'ok').
-spec(start_mirroring/1 :: (pid()) -> 'ok').
-spec(stop_mirroring/1 :: (pid()) -> 'ok').
-spec(sync_mirrors/1 :: (pid()) -> 'ok' | rabbit_types:error('not_mirrored')).
-spec(cancel_sync_mirrors/1 :: (pid()) -> 'ok' | {'ok', 'not_syncing'}).

-endif.

%%----------------------------------------------------------------------------

-define(CONSUMER_INFO_KEYS,
        [queue_name, channel_pid, consumer_tag, ack_required, prefetch_count,
         arguments]).

recover() ->
    %% Clear out remnants of old incarnation, in case we restarted
    %% faster than other nodes handled DOWN messages from us.
    on_node_down(node()),
    DurableQueues = find_durable_queues(),
    {ok, BQ} = application:get_env(rabbit, backing_queue_module),

    %% We rely on BQ:start/1 returning the recovery terms in the same
    %% order as the supplied queue names, so that we can zip them together
    %% for further processing in recover_durable_queues.
    {ok, OrderedRecoveryTerms} =
        BQ:start([QName || #amqqueue{name = QName} <- DurableQueues]),
    {ok,_} = supervisor:start_child(
               rabbit_sup,
               {rabbit_amqqueue_sup,
                {rabbit_amqqueue_sup, start_link, []},
                transient, infinity, supervisor, [rabbit_amqqueue_sup]}),
    recover_durable_queues(lists:zip(DurableQueues, OrderedRecoveryTerms)).

stop() ->
    ok = supervisor:terminate_child(rabbit_sup, rabbit_amqqueue_sup),
    ok = supervisor:delete_child(rabbit_sup, rabbit_amqqueue_sup),
    {ok, BQ} = application:get_env(rabbit, backing_queue_module),
    ok = BQ:stop().

start(Qs) ->
    %% At this point all recovered queues and their bindings are
    %% visible to routing, so now it is safe for them to complete
    %% their initialisation (which may involve interacting with other
    %% queues).
    [Pid ! {self(), go} || #amqqueue{pid = Pid} <- Qs],
    ok.

find_durable_queues() ->
    Node = node(),
    mnesia:async_dirty(
      fun () ->
              qlc:e(qlc:q([Q || Q = #amqqueue{name = Name,
                                              pid  = Pid}
                                    <- mnesia:table(rabbit_durable_queue),
                                node(Pid) == Node,
                                mnesia:read(rabbit_queue, Name, read) =:= []]))
      end).

recover_durable_queues(QueuesAndRecoveryTerms) ->
    {Results, Failures} =
        gen_server2:mcall([{start_queue_process(node(), Q),
                            {init, {self(), Terms}}} ||
                              {Q, Terms} <- QueuesAndRecoveryTerms]),
    [rabbit_log:error("Queue ~p failed to initialise: ~p~n",
                      [Pid, Error]) || {Pid, Error} <- Failures],
    [Q || {_, {new, Q}} <- Results].

declare(QueueName, Durable, AutoDelete, Args, Owner) ->
    declare(QueueName, Durable, AutoDelete, Args, Owner, node()).


%% The Node argument suggests where the queue (master if mirrored)
%% should be. Note that in some cases (e.g. with "nodes" policy in
%% effect) this might not be possible to satisfy.
declare(QueueName, Durable, AutoDelete, Args, Owner, Node) ->
    ok = check_declare_arguments(QueueName, Args),
    Q = rabbit_queue_decorator:set(
          rabbit_policy:set(#amqqueue{name             = QueueName,
                                      durable          = Durable,
                                      auto_delete      = AutoDelete,
                                      arguments        = Args,
                                      exclusive_owner  = Owner,
                                      pid              = none,
                                      slave_pids       = [],
                                      sync_slave_pids  = [],
                                      down_slave_nodes = [],
                                      gm_pids          = []})),
    Node = rabbit_mirror_queue_misc:initial_queue_node(Q, Node),
    gen_server2:call(start_queue_process(Node, Q), {init, new}, infinity).

internal_declare(Q, true) ->
    rabbit_misc:execute_mnesia_tx_with_tail(
      fun () -> ok = store_queue(Q), rabbit_misc:const(Q) end);
internal_declare(Q = #amqqueue{name = QueueName}, false) ->
    rabbit_misc:execute_mnesia_tx_with_tail(
      fun () ->
              case mnesia:wread({rabbit_queue, QueueName}) of
                  [] ->
                      case not_found_or_absent(QueueName) of
                          not_found        -> Q1 = rabbit_policy:set(Q),
                                              ok = store_queue(Q1),
                                              B = add_default_binding(Q1),
                                              fun () -> B(), Q1 end;
                          {absent, _Q} = R -> rabbit_misc:const(R)
                      end;
                  [ExistingQ = #amqqueue{pid = QPid}] ->
                      case rabbit_misc:is_process_alive(QPid) of
                          true  -> rabbit_misc:const(ExistingQ);
                          false -> TailFun = internal_delete(QueueName),
                                   fun () -> TailFun(), ExistingQ end
                      end
              end
      end).

update(Name, Fun) ->
    case mnesia:wread({rabbit_queue, Name}) of
        [Q = #amqqueue{durable = Durable}] ->
            Q1 = Fun(Q),
            ok = mnesia:write(rabbit_queue, Q1, write),
            case Durable of
                true -> ok = mnesia:write(rabbit_durable_queue, Q1, write);
                _    -> ok
            end,
            Q1;
        [] ->
            not_found
    end.

store_queue(Q = #amqqueue{durable = true}) ->
    ok = mnesia:write(rabbit_durable_queue,
                      Q#amqqueue{slave_pids      = [],
                                 sync_slave_pids = [],
                                 gm_pids         = [],
                                 decorators      = undefined}, write),
    store_queue_ram(Q);
store_queue(Q = #amqqueue{durable = false}) ->
    store_queue_ram(Q).

store_queue_ram(Q) ->
    ok = mnesia:write(rabbit_queue, rabbit_queue_decorator:set(Q), write).

update_decorators(Name) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() ->
              case mnesia:wread({rabbit_queue, Name}) of
                  [Q] -> store_queue_ram(Q),
                         ok;
                  []  -> ok
              end
      end).

policy_changed(Q1 = #amqqueue{decorators = Decorators1},
               Q2 = #amqqueue{decorators = Decorators2}) ->
    rabbit_mirror_queue_misc:update_mirrors(Q1, Q2),
    D1 = rabbit_queue_decorator:select(Decorators1),
    D2 = rabbit_queue_decorator:select(Decorators2),
    [ok = M:policy_changed(Q1, Q2) || M <- lists:usort(D1 ++ D2)],
    %% Make sure we emit a stats event even if nothing
    %% mirroring-related has changed - the policy may have changed anyway.
    notify_policy_changed(Q1).

start_queue_process(Node, Q) ->
    {ok, Pid} = rabbit_amqqueue_sup:start_child(Node, [Q]),
    Pid.

add_default_binding(#amqqueue{name = QueueName}) ->
    ExchangeName = rabbit_misc:r(QueueName, exchange, <<>>),
    RoutingKey = QueueName#resource.name,
    rabbit_binding:add(#binding{source      = ExchangeName,
                                destination = QueueName,
                                key         = RoutingKey,
                                args        = []}).

lookup([])     -> [];                             %% optimisation
lookup([Name]) -> ets:lookup(rabbit_queue, Name); %% optimisation
lookup(Names) when is_list(Names) ->
    %% Normally we'd call mnesia:dirty_read/1 here, but that is quite
    %% expensive for reasons explained in rabbit_misc:dirty_read/1.
    lists:append([ets:lookup(rabbit_queue, Name) || Name <- Names]);
lookup(Name) ->
    rabbit_misc:dirty_read({rabbit_queue, Name}).

not_found_or_absent(Name) ->
    %% NB: we assume that the caller has already performed a lookup on
    %% rabbit_queue and not found anything
    case mnesia:read({rabbit_durable_queue, Name}) of
        []  -> not_found;
        [Q] -> {absent, Q} %% Q exists on stopped node
    end.

not_found_or_absent_dirty(Name) ->
    %% We should read from both tables inside a tx, to get a
    %% consistent view. But the chances of an inconsistency are small,
    %% and only affect the error kind.
    case rabbit_misc:dirty_read({rabbit_durable_queue, Name}) of
        {error, not_found} -> not_found;
        {ok, Q}            -> {absent, Q}
    end.

with(Name, F, E) ->
    case lookup(Name) of
        {ok, Q = #amqqueue{pid = QPid}} ->
            %% We check is_process_alive(QPid) in case we receive a
            %% nodedown (for example) in F() that has nothing to do
            %% with the QPid. F() should be written s.t. that this
            %% cannot happen, so we bail if it does since that
            %% indicates a code bug and we don't want to get stuck in
            %% the retry loop.
            rabbit_misc:with_exit_handler(
              fun () -> false = rabbit_misc:is_process_alive(QPid),
                        timer:sleep(25),
                        with(Name, F, E)
              end, fun () -> F(Q) end);
        {error, not_found} ->
            E(not_found_or_absent_dirty(Name))
    end.

with(Name, F) -> with(Name, F, fun (E) -> {error, E} end).

with_or_die(Name, F) ->
    with(Name, F, fun (not_found)   -> rabbit_misc:not_found(Name);
                      ({absent, Q}) -> rabbit_misc:absent(Q)
                  end).

assert_equivalence(#amqqueue{durable     = Durable,
                             auto_delete = AutoDelete} = Q,
                   Durable, AutoDelete, RequiredArgs, Owner) ->
    assert_args_equivalence(Q, RequiredArgs),
    check_exclusive_access(Q, Owner, strict);
assert_equivalence(#amqqueue{name = QueueName},
                   _Durable, _AutoDelete, _RequiredArgs, _Owner) ->
    rabbit_misc:protocol_error(
      precondition_failed, "parameters for ~s not equivalent",
      [rabbit_misc:rs(QueueName)]).

check_exclusive_access(Q, Owner) -> check_exclusive_access(Q, Owner, lax).

check_exclusive_access(#amqqueue{exclusive_owner = Owner}, Owner, _MatchType) ->
    ok;
check_exclusive_access(#amqqueue{exclusive_owner = none}, _ReaderPid, lax) ->
    ok;
check_exclusive_access(#amqqueue{name = QueueName}, _ReaderPid, _MatchType) ->
    rabbit_misc:protocol_error(
      resource_locked,
      "cannot obtain exclusive access to locked ~s",
      [rabbit_misc:rs(QueueName)]).

with_exclusive_access_or_die(Name, ReaderPid, F) ->
    with_or_die(Name,
                fun (Q) -> check_exclusive_access(Q, ReaderPid), F(Q) end).

assert_args_equivalence(#amqqueue{name = QueueName, arguments = Args},
                        RequiredArgs) ->
    rabbit_misc:assert_args_equivalence(Args, RequiredArgs, QueueName,
                                        [Key || {Key, _Fun} <- declare_args()]).

check_declare_arguments(QueueName, Args) ->
    check_arguments(QueueName, Args, declare_args()).

check_consume_arguments(QueueName, Args) ->
    check_arguments(QueueName, Args, consume_args()).

check_arguments(QueueName, Args, Validators) ->
    [case rabbit_misc:table_lookup(Args, Key) of
         undefined -> ok;
         TypeVal   -> case Fun(TypeVal, Args) of
                          ok             -> ok;
                          {error, Error} -> rabbit_misc:protocol_error(
                                              precondition_failed,
                                              "invalid arg '~s' for ~s: ~255p",
                                              [Key, rabbit_misc:rs(QueueName),
                                               Error])
                      end
     end || {Key, Fun} <- Validators],
    ok.

declare_args() ->
    [{<<"x-expires">>,                 fun check_expires_arg/2},
     {<<"x-message-ttl">>,             fun check_message_ttl_arg/2},
     {<<"x-dead-letter-routing-key">>, fun check_dlxrk_arg/2},
     {<<"x-max-length">>,              fun check_non_neg_int_arg/2}].

consume_args() -> [{<<"x-priority">>,              fun check_int_arg/2},
                   {<<"x-cancel-on-ha-failover">>, fun check_bool_arg/2}].

check_int_arg({Type, _}, _) ->
    case lists:member(Type, ?INTEGER_ARG_TYPES) of
        true  -> ok;
        false -> {error, {unacceptable_type, Type}}
    end.

check_bool_arg({bool, _}, _) -> ok;
check_bool_arg({Type, _}, _) -> {error, {unacceptable_type, Type}}.

check_non_neg_int_arg({Type, Val}, Args) ->
    case check_int_arg({Type, Val}, Args) of
        ok when Val >= 0 -> ok;
        ok               -> {error, {value_negative, Val}};
        Error            -> Error
    end.

check_expires_arg({Type, Val}, Args) ->
    case check_int_arg({Type, Val}, Args) of
        ok when Val == 0 -> {error, {value_zero, Val}};
        ok               -> rabbit_misc:check_expiry(Val);
        Error            -> Error
    end.

check_message_ttl_arg({Type, Val}, Args) ->
    case check_int_arg({Type, Val}, Args) of
        ok    -> rabbit_misc:check_expiry(Val);
        Error -> Error
    end.

check_dlxrk_arg({longstr, _}, Args) ->
    case rabbit_misc:table_lookup(Args, <<"x-dead-letter-exchange">>) of
        undefined -> {error, routing_key_but_no_dlx_defined};
        _         -> ok
    end;
check_dlxrk_arg({Type,    _}, _Args) ->
    {error, {unacceptable_type, Type}}.

list() -> mnesia:dirty_match_object(rabbit_queue, #amqqueue{_ = '_'}).

%% Not dirty_match_object since that would not be transactional when used in a
%% tx context
list(VHostPath) ->
    mnesia:async_dirty(
      fun () ->
              mnesia:match_object(
                rabbit_queue,
                #amqqueue{name = rabbit_misc:r(VHostPath, queue), _ = '_'},
                read)
      end).

info_keys() -> rabbit_amqqueue_process:info_keys().

map(VHostPath, F) -> rabbit_misc:filter_exit_map(F, list(VHostPath)).

info(#amqqueue{ pid = QPid }) -> delegate:call(QPid, info).

info(#amqqueue{ pid = QPid }, Items) ->
    case delegate:call(QPid, {info, Items}) of
        {ok, Res}      -> Res;
        {error, Error} -> throw(Error)
    end.

info_all(VHostPath) -> map(VHostPath, fun (Q) -> info(Q) end).

info_all(VHostPath, Items) -> map(VHostPath, fun (Q) -> info(Q, Items) end).

force_event_refresh(Ref) ->
    [gen_server2:cast(Q#amqqueue.pid,
                      {force_event_refresh, Ref}) || Q <- list()],
    ok.

notify_policy_changed(#amqqueue{pid = QPid}) ->
    gen_server2:cast(QPid, policy_changed).

consumers(#amqqueue{ pid = QPid }) -> delegate:call(QPid, consumers).

consumer_info_keys() -> ?CONSUMER_INFO_KEYS.

consumers_all(VHostPath) ->
    ConsumerInfoKeys=consumer_info_keys(),
    lists:append(
      map(VHostPath,
          fun (Q) ->
              [lists:zip(
                 ConsumerInfoKeys,
                 [Q#amqqueue.name, ChPid, CTag, AckRequired, Prefetch, Args]) ||
                  {ChPid, CTag, AckRequired, Prefetch, Args} <- consumers(Q)]
          end)).

stat(#amqqueue{pid = QPid}) -> delegate:call(QPid, stat).

delete_immediately(QPids) ->
    [gen_server2:cast(QPid, delete_immediately) || QPid <- QPids],
    ok.

delete(#amqqueue{ pid = QPid }, IfUnused, IfEmpty) ->
    delegate:call(QPid, {delete, IfUnused, IfEmpty}).

purge(#amqqueue{ pid = QPid }) -> delegate:call(QPid, purge).

deliver(Qs, Delivery) -> deliver(Qs, Delivery, noflow).

deliver_flow(Qs, Delivery) -> deliver(Qs, Delivery, flow).

requeue(QPid, MsgIds, ChPid) -> delegate:call(QPid, {requeue, MsgIds, ChPid}).

ack(QPid, MsgIds, ChPid) -> delegate:cast(QPid, {ack, MsgIds, ChPid}).

reject(QPid, Requeue, MsgIds, ChPid) ->
    delegate:cast(QPid, {reject, Requeue, MsgIds, ChPid}).

notify_down_all(QPids, ChPid) ->
    {_, Bads} = delegate:call(QPids, {notify_down, ChPid}),
    case lists:filter(
           fun ({_Pid, {exit, {R, _}, _}}) -> rabbit_misc:is_abnormal_exit(R);
               ({_Pid, _})                 -> false
           end, Bads) of
        []    -> ok;
        Bads1 -> {error, Bads1}
    end.

activate_limit_all(QPids, ChPid) ->
    delegate:cast(QPids, {activate_limit, ChPid}).

credit(#amqqueue{pid = QPid}, ChPid, CTag, Credit, Drain) ->
    delegate:cast(QPid, {credit, ChPid, CTag, Credit, Drain}).

basic_get(#amqqueue{pid = QPid}, ChPid, NoAck, LimiterPid) ->
    delegate:call(QPid, {basic_get, ChPid, NoAck, LimiterPid}).

basic_consume(#amqqueue{pid = QPid, name = QName}, NoAck, ChPid, LimiterPid,
              LimiterActive, ConsumerPrefetchCount, ConsumerTag,
              ExclusiveConsume, Args, OkMsg) ->
    ok = check_consume_arguments(QName, Args),
    delegate:call(QPid, {basic_consume, NoAck, ChPid, LimiterPid, LimiterActive,
                         ConsumerPrefetchCount, ConsumerTag, ExclusiveConsume,
                         Args, OkMsg}).

basic_cancel(#amqqueue{pid = QPid}, ChPid, ConsumerTag, OkMsg) ->
    delegate:call(QPid, {basic_cancel, ChPid, ConsumerTag, OkMsg}).

notify_decorators(#amqqueue{pid = QPid}) ->
    delegate:cast(QPid, notify_decorators).

notify_sent(QPid, ChPid) ->
    Key = {consumer_credit_to, QPid},
    put(Key, case get(Key) of
                 1         -> gen_server2:cast(
                                QPid, {notify_sent, ChPid,
                                       ?MORE_CONSUMER_CREDIT_AFTER}),
                              ?MORE_CONSUMER_CREDIT_AFTER;
                 undefined -> erlang:monitor(process, QPid),
                              ?MORE_CONSUMER_CREDIT_AFTER - 1;
                 C         -> C - 1
             end),
    ok.

notify_sent_queue_down(QPid) ->
    erase({consumer_credit_to, QPid}),
    ok.

resume(QPid, ChPid) -> delegate:cast(QPid, {resume, ChPid}).

internal_delete1(QueueName) ->
    ok = mnesia:delete({rabbit_queue, QueueName}),
    %% this 'guarded' delete prevents unnecessary writes to the mnesia
    %% disk log
    case mnesia:wread({rabbit_durable_queue, QueueName}) of
        []  -> ok;
        [_] -> ok = mnesia:delete({rabbit_durable_queue, QueueName})
    end,
    %% we want to execute some things, as decided by rabbit_exchange,
    %% after the transaction.
    rabbit_binding:remove_for_destination(QueueName).

internal_delete(QueueName) ->
    rabbit_misc:execute_mnesia_tx_with_tail(
      fun () ->
              case {mnesia:wread({rabbit_queue, QueueName}),
                    mnesia:wread({rabbit_durable_queue, QueueName})} of
                  {[], []} ->
                      rabbit_misc:const({error, not_found});
                  _ ->
                      Deletions = internal_delete1(QueueName),
                      T = rabbit_binding:process_deletions(Deletions),
                      fun() ->
                              ok = T(),
                              ok = rabbit_event:notify(queue_deleted,
                                                       [{name, QueueName}])
                      end
              end
      end).

forget_all_durable(Node) ->
    %% Note rabbit is not running so we avoid e.g. the worker pool. Also why
    %% we don't invoke the return from rabbit_binding:process_deletions/1.
    {atomic, ok} =
        mnesia:sync_transaction(
          fun () ->
                  Qs = mnesia:match_object(rabbit_durable_queue,
                                           #amqqueue{_ = '_'}, write),
                  [forget_node_for_queue(Q) || #amqqueue{pid = Pid} = Q <- Qs,
                      node(Pid) =:= Node],
                  ok
          end),
    ok.

forget_node_for_queue(#amqqueue{name             = Name,
                                down_slave_nodes = []}) ->
    %% No slaves to recover from, queue is gone
    rabbit_binding:process_deletions(internal_delete1(Name));

forget_node_for_queue(Q = #amqqueue{down_slave_nodes = [H|T]}) ->
    %% Promote a slave while down - it'll happily recover as a master
    Q1 = Q#amqqueue{pid              = rabbit_misc:node_to_fake_pid(H),
                    down_slave_nodes = T},
    ok = mnesia:write(rabbit_durable_queue, Q1, write).

run_backing_queue(QPid, Mod, Fun) ->
    gen_server2:cast(QPid, {run_backing_queue, Mod, Fun}).

set_ram_duration_target(QPid, Duration) ->
    gen_server2:cast(QPid, {set_ram_duration_target, Duration}).

set_maximum_since_use(QPid, Age) ->
    gen_server2:cast(QPid, {set_maximum_since_use, Age}).

start_mirroring(QPid) -> ok = delegate:cast(QPid, start_mirroring).
stop_mirroring(QPid)  -> ok = delegate:cast(QPid, stop_mirroring).

sync_mirrors(QPid)        -> delegate:call(QPid, sync_mirrors).
cancel_sync_mirrors(QPid) -> delegate:call(QPid, cancel_sync_mirrors).

on_node_up(Node) ->
    ok = rabbit_misc:execute_mnesia_transaction(
           fun () ->
                   Qs = mnesia:match_object(rabbit_queue,
                                            #amqqueue{_ = '_'}, write),
                   [case lists:member(Node, DSNs) of
                        true  -> DSNs1 = DSNs -- [Node],
                                 store_queue(
                                   Q#amqqueue{down_slave_nodes = DSNs1});
                        false -> ok
                    end || #amqqueue{down_slave_nodes = DSNs} = Q <- Qs],
                   ok
           end).

on_node_down(Node) ->
    rabbit_misc:execute_mnesia_tx_with_tail(
      fun () -> QsDels =
                    qlc:e(qlc:q([{QName, delete_queue(QName)} ||
                                    #amqqueue{name = QName, pid = Pid,
                                              slave_pids = []}
                                        <- mnesia:table(rabbit_queue),
                                    node(Pid) == Node andalso
                                    not rabbit_misc:is_process_alive(Pid)])),
                {Qs, Dels} = lists:unzip(QsDels),
                T = rabbit_binding:process_deletions(
                      lists:foldl(fun rabbit_binding:combine_deletions/2,
                                  rabbit_binding:new_deletions(), Dels)),
                fun () ->
                        T(),
                        lists:foreach(
                          fun(QName) ->
                                  ok = rabbit_event:notify(queue_deleted,
                                                           [{name, QName}])
                          end, Qs)
                end
      end).

delete_queue(QueueName) ->
    ok = mnesia:delete({rabbit_queue, QueueName}),
    rabbit_binding:remove_transient_for_destination(QueueName).

pseudo_queue(QueueName, Pid) ->
    #amqqueue{name         = QueueName,
              durable      = false,
              auto_delete  = false,
              arguments    = [],
              pid          = Pid,
              slave_pids   = []}.

immutable(Q) -> Q#amqqueue{pid              = none,
                           slave_pids       = none,
                           sync_slave_pids  = none,
                           down_slave_nodes = none,
                           gm_pids          = none,
                           policy           = none,
                           decorators       = none}.

deliver([], _Delivery, _Flow) ->
    %% /dev/null optimisation
    [];

deliver(Qs, Delivery, Flow) ->
    {MPids, SPids} = qpids(Qs),
    QPids = MPids ++ SPids,
    case Flow of
        flow   -> [credit_flow:send(QPid) || QPid <- QPids];
        noflow -> ok
    end,

    %% We let slaves know that they were being addressed as slaves at
    %% the time - if they receive such a message from the channel
    %% after they have become master they should mark the message as
    %% 'delivered' since they do not know what the master may have
    %% done with it.
    MMsg = {deliver, Delivery, false, Flow},
    SMsg = {deliver, Delivery, true,  Flow},
    delegate:cast(MPids, MMsg),
    delegate:cast(SPids, SMsg),
    QPids.

qpids([]) -> {[], []}; %% optimisation
qpids([#amqqueue{pid = QPid, slave_pids = SPids}]) -> {[QPid], SPids}; %% opt
qpids(Qs) ->
    {MPids, SPids} = lists:foldl(fun (#amqqueue{pid = QPid, slave_pids = SPids},
                                      {MPidAcc, SPidAcc}) ->
                                         {[QPid | MPidAcc], [SPids | SPidAcc]}
                                 end, {[], []}, Qs),
    {MPids, lists:append(SPids)}.
