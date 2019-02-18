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
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_amqqueue).

-export([warn_file_limit/0]).
-export([recover/1, stop/1, start/1, declare/6, declare/7,
         delete_immediately/1, delete_exclusive/2, delete/4, purge/1,
         forget_all_durable/1, delete_crashed/1, delete_crashed/2,
         delete_crashed_internal/2]).
-export([pseudo_queue/2, pseudo_queue/3, immutable/1]).
-export([lookup/1, not_found_or_absent/1, with/2, with/3, with_or_die/2,
         assert_equivalence/5,
         check_exclusive_access/2, with_exclusive_access_or_die/3,
         stat/1, deliver/2, deliver/3, requeue/4, ack/4, reject/5]).
-export([not_found/1, absent/2]).
-export([list/0, list/1, info_keys/0, info/1, info/2, info_all/1, info_all/2,
         emit_info_all/5, list_local/1, info_local/1,
         emit_info_local/4, emit_info_down/4]).
-export([list_down/1, count/1, list_names/0, list_names/1, list_local_names/0,
         list_with_possible_retry/1]).
-export([list_by_type/1]).
-export([force_event_refresh/1, notify_policy_changed/1]).
-export([consumers/1, consumers_all/1,  emit_consumers_all/4, consumer_info_keys/0]).
-export([basic_get/6, basic_consume/12, basic_cancel/6, notify_decorators/1]).
-export([notify_sent/2, notify_sent_queue_down/1, resume/2]).
-export([notify_down_all/2, notify_down_all/3, activate_limit_all/2, credit/6]).
-export([on_node_up/1, on_node_down/1]).
-export([update/2, store_queue/1, update_decorators/1, policy_changed/2]).
-export([update_mirroring/1, sync_mirrors/1, cancel_sync_mirrors/1]).
-export([emit_unresponsive/6, emit_unresponsive_local/5, is_unresponsive/2]).
-export([is_replicated/1, is_dead_exclusive/1]). % Note: exported due to use in qlc expression.
-export([list_local_followers/0]).
-export([ensure_rabbit_queue_record_is_initialized/1]).
-export([format/1]).
-export([delete_immediately_by_resource/1]).

-export([pid_of/1, pid_of/2]).
-export([mark_local_durable_queues_stopped/1]).

-deprecated([{force_event_refresh, 1, eventually}]).

%% internal
-export([internal_declare/2, internal_delete/2, run_backing_queue/3,
         set_ram_duration_target/2, set_maximum_since_use/2,
         emit_consumers_local/3, internal_delete/3]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include("amqqueue.hrl").

-define(INTEGER_ARG_TYPES, [byte, short, signedint, long,
                            unsignedbyte, unsignedshort, unsignedint]).

-define(MORE_CONSUMER_CREDIT_AFTER, 50).

-define(IS_CLASSIC(QPid), is_pid(QPid)).
-define(IS_QUORUM(QPid), is_tuple(QPid)).
%%----------------------------------------------------------------------------

-export_type([name/0, qmsg/0, absent_reason/0]).

-type name() :: rabbit_types:r('queue').
-type qpids() :: [pid()].
-type qlen() :: rabbit_types:ok(non_neg_integer()).
-type qfun(A) :: fun ((amqqueue:amqqueue()) -> A | no_return()).
-type qmsg() :: {name(), pid() | {atom(), pid()}, msg_id(), boolean(), rabbit_types:message()}.
-type msg_id() :: non_neg_integer().
-type ok_or_errors() ::
        'ok' | {'error', [{'error' | 'exit' | 'throw', any()}]}.
-type absent_reason() :: 'nodedown' | 'crashed' | stopped | timeout.
-type queue_not_found() :: not_found.
-type queue_absent() :: {'absent', amqqueue:amqqueue(), absent_reason()}.
-type not_found_or_absent() :: queue_not_found() | queue_absent().
-type quorum_states() :: #{Name :: atom() => rabbit_fifo_client:state()}.

%%----------------------------------------------------------------------------

-define(CONSUMER_INFO_KEYS,
        [queue_name, channel_pid, consumer_tag, ack_required, prefetch_count,
         active, activity_status, arguments]).

warn_file_limit() ->
    DurableQueues = find_recoverable_queues(),
    L = length(DurableQueues),

    %% if there are not enough file handles, the server might hang
    %% when trying to recover queues, warn the user:
    case file_handle_cache:get_limit() < L of
        true ->
            rabbit_log:warning(
              "Recovering ~p queues, available file handles: ~p. Please increase max open file handles limit to at least ~p!~n",
              [L, file_handle_cache:get_limit(), L]);
        false ->
            ok
    end.

-spec recover(rabbit_types:vhost()) ->
    {RecoveredClassic :: [amqqueue:amqqueue()],
     FailedClassic :: [amqqueue:amqqueue()],
     Quorum :: [amqqueue:amqqueue()]}.

recover(VHost) ->
    AllClassic = find_local_durable_classic_queues(VHost),
    Quorum = find_local_quorum_queues(VHost),
    {RecoveredClassic, FailedClassic} = recover_classic_queues(VHost, AllClassic),
    {RecoveredClassic, FailedClassic, rabbit_quorum_queue:recover(Quorum)}.

recover_classic_queues(VHost, Queues) ->
    {ok, BQ} = application:get_env(rabbit, backing_queue_module),
    %% We rely on BQ:start/1 returning the recovery terms in the same
    %% order as the supplied queue names, so that we can zip them together
    %% for further processing in recover_durable_queues.
    {ok, OrderedRecoveryTerms} =
        BQ:start(VHost, [amqqueue:get_name(Q) || Q <- Queues]),
    case rabbit_amqqueue_sup_sup:start_for_vhost(VHost) of
        {ok, _}         ->
            RecoveredQs = recover_durable_queues(lists:zip(Queues, OrderedRecoveryTerms)),
            RecoveredNames = [amqqueue:get_name(Q) || Q <- RecoveredQs],
            FailedQueues = [Q || Q <- Queues,
                                 not lists:member(amqqueue:get_name(Q), RecoveredNames)],
            {RecoveredQs, FailedQueues};
        {error, Reason} ->
            rabbit_log:error("Failed to start queue supervisor for vhost '~s': ~s", [VHost, Reason]),
            throw({error, Reason})
    end.

filter_pid_per_type(QPids) ->
    lists:partition(fun(QPid) -> ?IS_CLASSIC(QPid) end, QPids).

filter_resource_per_type(Resources) ->
    Queues = [begin
                  {ok, Q} = lookup(Resource),
                  QPid = amqqueue:get_pid(Q),
                  {Resource, QPid}
              end || Resource <- Resources],
    lists:partition(fun({_Resource, QPid}) -> ?IS_CLASSIC(QPid) end, Queues).

-spec stop(rabbit_types:vhost()) -> 'ok'.

stop(VHost) ->
    %% Classic queues
    ok = rabbit_amqqueue_sup_sup:stop_for_vhost(VHost),
    {ok, BQ} = application:get_env(rabbit, backing_queue_module),
    ok = BQ:stop(VHost),
    rabbit_quorum_queue:stop(VHost).

-spec start([amqqueue:amqqueue()]) -> 'ok'.

start(Qs) ->
    %% At this point all recovered queues and their bindings are
    %% visible to routing, so now it is safe for them to complete
    %% their initialisation (which may involve interacting with other
    %% queues).
    _ = [amqqueue:get_pid(Q) ! {self(), go}
         || Q <- Qs,
            %% All queues are supposed to be classic here.
            amqqueue:is_classic(Q)],
    ok.

mark_local_durable_queues_stopped(VHost) ->
    ?try_mnesia_tx_or_upgrade_amqqueue_and_retry(
       do_mark_local_durable_queues_stopped(VHost),
       do_mark_local_durable_queues_stopped(VHost)).

do_mark_local_durable_queues_stopped(VHost) ->
    Qs = find_local_durable_classic_queues(VHost),
    rabbit_misc:execute_mnesia_transaction(
        fun() ->
            [ store_queue(amqqueue:set_state(Q, stopped))
              || Q <- Qs,
                 amqqueue:get_state(Q) =/= stopped ]
        end).

find_local_quorum_queues(VHost) ->
    Node = node(),
    mnesia:async_dirty(
      fun () ->
              qlc:e(qlc:q([Q || Q <- mnesia:table(rabbit_durable_queue),
                                amqqueue:get_vhost(Q) =:= VHost,
                                amqqueue:is_quorum(Q) andalso
                                (lists:member(Node, amqqueue:get_quorum_nodes(Q)))]))
      end).

find_local_durable_classic_queues(VHost) ->
    Node = node(),
    mnesia:async_dirty(
      fun () ->
              qlc:e(qlc:q([Q || Q <- mnesia:table(rabbit_durable_queue),
                                amqqueue:get_vhost(Q) =:= VHost,
                                amqqueue:is_classic(Q) andalso
                                (is_local_to_node(amqqueue:get_pid(Q), Node) andalso
                                 %% Terminations on node down will not remove the rabbit_queue
                                 %% record if it is a mirrored queue (such info is now obtained from
                                 %% the policy). Thus, we must check if the local pid is alive
                                 %% - if the record is present - in order to restart.
                                 (mnesia:read(rabbit_queue, amqqueue:get_name(Q), read) =:= []
                                  orelse not rabbit_mnesia:is_process_alive(amqqueue:get_pid(Q))))
                          ]))
      end).

find_recoverable_queues() ->
    Node = node(),
    mnesia:async_dirty(
      fun () ->
              qlc:e(qlc:q([Q || Q <- mnesia:table(rabbit_durable_queue),
                                (amqqueue:is_classic(Q) andalso
                                      (is_local_to_node(amqqueue:get_pid(Q), Node) andalso
                                       %% Terminations on node down will not remove the rabbit_queue
                                       %% record if it is a mirrored queue (such info is now obtained from
                                       %% the policy). Thus, we must check if the local pid is alive
                                       %% - if the record is present - in order to restart.
                                             (mnesia:read(rabbit_queue, amqqueue:get_name(Q), read) =:= []
                                              orelse not rabbit_mnesia:is_process_alive(amqqueue:get_pid(Q)))))
                                    orelse (amqqueue:is_quorum(Q) andalso lists:member(Node, amqqueue:get_quorum_nodes(Q)))
                          ]))
      end).

recover_durable_queues(QueuesAndRecoveryTerms) ->
    {Results, Failures} =
        gen_server2:mcall(
          [{rabbit_amqqueue_sup_sup:start_queue_process(node(), Q, recovery),
            {init, {self(), Terms}}} || {Q, Terms} <- QueuesAndRecoveryTerms]),
    [rabbit_log:error("Queue ~p failed to initialise: ~p~n",
                      [Pid, Error]) || {Pid, Error} <- Failures],
    [Q || {_, {new, Q}} <- Results].

-spec declare(name(),
              boolean(),
              boolean(),
              rabbit_framing:amqp_table(),
              rabbit_types:maybe(pid()),
              rabbit_types:username()) ->
    {'new' | 'existing' | 'owner_died', amqqueue:amqqueue()} |
    {'new', amqqueue:amqqueue(), rabbit_fifo_client:state()} |
    {'absent', amqqueue:amqqueue(), absent_reason()} |
    rabbit_types:channel_exit().

declare(QueueName, Durable, AutoDelete, Args, Owner, ActingUser) ->
    declare(QueueName, Durable, AutoDelete, Args, Owner, ActingUser, node()).


%% The Node argument suggests where the queue (master if mirrored)
%% should be. Note that in some cases (e.g. with "nodes" policy in
%% effect) this might not be possible to satisfy.

-spec declare(name(),
              boolean(),
              boolean(),
              rabbit_framing:amqp_table(),
              rabbit_types:maybe(pid()),
              rabbit_types:username(),
              node()) ->
    {'new' | 'existing' | 'owner_died', amqqueue:amqqueue()} |
    {'new', amqqueue:amqqueue(), rabbit_fifo_client:state()} |
    {'absent', amqqueue:amqqueue(), absent_reason()} |
    rabbit_types:channel_exit().

declare(QueueName = #resource{virtual_host = VHost}, Durable, AutoDelete, Args,
        Owner, ActingUser, Node) ->
    ok = check_declare_arguments(QueueName, Args),
    Type = get_queue_type(Args),
    TypeIsAllowed =
      Type =:= classic orelse
      rabbit_feature_flags:is_enabled(quorum_queue),
    case TypeIsAllowed of
        true ->
            Q0 = amqqueue:new(QueueName,
                              none,
                              Durable,
                              AutoDelete,
                              Owner,
                              Args,
                              VHost,
                              #{user => ActingUser},
                              Type),
            Q = rabbit_queue_decorator:set(
                  rabbit_policy:set(Q0)),
            do_declare(Q, Node);
        false ->
            rabbit_misc:protocol_error(
              internal_error,
              "Cannot declare a queue '~s' of type '~s' on node '~s': "
              "the 'quorum_queue' feature flag is disabled",
              [rabbit_misc:rs(QueueName), Type, Node])
    end.

do_declare(Q, Node) when ?amqqueue_is_classic(Q) ->
    declare_classic_queue(Q, Node);
do_declare(Q, _Node) when ?amqqueue_is_quorum(Q) ->
    rabbit_quorum_queue:declare(Q).

declare_classic_queue(Q, Node) ->
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

get_queue_type(Args) ->
    case rabbit_misc:table_lookup(Args, <<"x-queue-type">>) of
        undefined ->
            classic;
        {_, V} ->
            erlang:binary_to_existing_atom(V, utf8)
    end.

-spec internal_declare(amqqueue:amqqueue(), boolean()) ->
    {created | existing, amqqueue:amqqueue()} | queue_absent().

internal_declare(Q, Recover) ->
    ?try_mnesia_tx_or_upgrade_amqqueue_and_retry(
        do_internal_declare(Q, Recover),
        begin
            Q1 = amqqueue:upgrade(Q),
            do_internal_declare(Q1, Recover)
        end).

do_internal_declare(Q, true) ->
    rabbit_misc:execute_mnesia_tx_with_tail(
      fun () ->
              ok = store_queue(amqqueue:set_state(Q, live)),
              rabbit_misc:const({created, Q})
      end);
do_internal_declare(Q, false) ->
    QueueName = amqqueue:get_name(Q),
    rabbit_misc:execute_mnesia_tx_with_tail(
      fun () ->
              case mnesia:wread({rabbit_queue, QueueName}) of
                  [] ->
                      case not_found_or_absent(QueueName) of
                          not_found           -> Q1 = rabbit_policy:set(Q),
                                                 Q2 = amqqueue:set_state(Q1, live),
                                                 ok = store_queue(Q2),
                                                 fun () -> {created, Q2} end;
                          {absent, _Q, _} = R -> rabbit_misc:const(R)
                      end;
                  [ExistingQ] ->
                      rabbit_misc:const({existing, ExistingQ})
              end
      end).

-spec update
        (name(), fun((amqqueue:amqqueue()) -> amqqueue:amqqueue())) ->
            'not_found' | amqqueue:amqqueue().

update(Name, Fun) ->
    case mnesia:wread({rabbit_queue, Name}) of
        [Q] ->
            Durable = amqqueue:is_durable(Q),
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

%% only really used for quorum queues to ensure the rabbit_queue record
%% is initialised
ensure_rabbit_queue_record_is_initialized(Q) ->
    ?try_mnesia_tx_or_upgrade_amqqueue_and_retry(
       do_ensure_rabbit_queue_record_is_initialized(Q),
       begin
           Q1 = amqqueue:upgrade(Q),
           do_ensure_rabbit_queue_record_is_initialized(Q1)
       end).

do_ensure_rabbit_queue_record_is_initialized(Q) ->
    rabbit_misc:execute_mnesia_tx_with_tail(
      fun () ->
              ok = store_queue(Q),
              rabbit_misc:const({ok, Q})
      end).

-spec store_queue(amqqueue:amqqueue()) -> 'ok'.

store_queue(Q) when ?amqqueue_is_durable(Q) ->
    Q1 = amqqueue:reset_mirroring_and_decorators(Q),
    ok = mnesia:write(rabbit_durable_queue, Q1, write),
    store_queue_ram(Q);
store_queue(Q) when not ?amqqueue_is_durable(Q) ->
    store_queue_ram(Q).

store_queue_ram(Q) ->
    ok = mnesia:write(rabbit_queue, rabbit_queue_decorator:set(Q), write).

-spec update_decorators(name()) -> 'ok'.

update_decorators(Name) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() ->
              case mnesia:wread({rabbit_queue, Name}) of
                  [Q] -> store_queue_ram(Q),
                         ok;
                  []  -> ok
              end
      end).

-spec policy_changed(amqqueue:amqqueue(), amqqueue:amqqueue()) ->
          'ok'.

policy_changed(Q1, Q2) ->
    Decorators1 = amqqueue:get_decorators(Q1),
    Decorators2 = amqqueue:get_decorators(Q2),
    rabbit_mirror_queue_misc:update_mirrors(Q1, Q2),
    D1 = rabbit_queue_decorator:select(Decorators1),
    D2 = rabbit_queue_decorator:select(Decorators2),
    [ok = M:policy_changed(Q1, Q2) || M <- lists:usort(D1 ++ D2)],
    %% Make sure we emit a stats event even if nothing
    %% mirroring-related has changed - the policy may have changed anyway.
    notify_policy_changed(Q1).

-spec lookup
        (name()) ->
            rabbit_types:ok(amqqueue:amqqueue()) |
            rabbit_types:error('not_found');
        ([name()]) ->
            [amqqueue:amqqueue()].

lookup([])     -> [];                             %% optimisation
lookup([Name]) -> ets:lookup(rabbit_queue, Name); %% optimisation
lookup(Names) when is_list(Names) ->
    %% Normally we'd call mnesia:dirty_read/1 here, but that is quite
    %% expensive for reasons explained in rabbit_misc:dirty_read/1.
    lists:append([ets:lookup(rabbit_queue, Name) || Name <- Names]);
lookup(Name) ->
    rabbit_misc:dirty_read({rabbit_queue, Name}).

-spec not_found_or_absent(name()) -> not_found_or_absent().

not_found_or_absent(Name) ->
    %% NB: we assume that the caller has already performed a lookup on
    %% rabbit_queue and not found anything
    case mnesia:read({rabbit_durable_queue, Name}) of
        []  -> not_found;
        [Q] -> {absent, Q, nodedown} %% Q exists on stopped node
    end.

-spec not_found_or_absent_dirty(name()) -> not_found_or_absent().

not_found_or_absent_dirty(Name) ->
    %% We should read from both tables inside a tx, to get a
    %% consistent view. But the chances of an inconsistency are small,
    %% and only affect the error kind.
    case rabbit_misc:dirty_read({rabbit_durable_queue, Name}) of
        {error, not_found} -> not_found;
        {ok, Q}            -> {absent, Q, nodedown}
    end.

-spec with(name(),
           qfun(A),
           fun((not_found_or_absent()) -> rabbit_types:channel_exit())) ->
    A | rabbit_types:channel_exit().

with(Name, F, E) ->
    with(Name, F, E, 2000).

with(#resource{} = Name, F, E, RetriesLeft) ->
    case lookup(Name) of
        {ok, Q} when ?amqqueue_state_is(Q, live) andalso RetriesLeft =:= 0 ->
            %% Something bad happened to that queue, we are bailing out
            %% on processing current request.
            E({absent, Q, timeout});
        {ok, Q} when ?amqqueue_state_is(Q, stopped) andalso RetriesLeft =:= 0 ->
            %% The queue was stopped and not migrated
            E({absent, Q, stopped});
        %% The queue process has crashed with unknown error
        {ok, Q} when ?amqqueue_state_is(Q, crashed) ->
            E({absent, Q, crashed});
        %% The queue process has been stopped by a supervisor.
        %% In that case a synchronised slave can take over
        %% so we should retry.
        {ok, Q} when ?amqqueue_state_is(Q, stopped) ->
            %% The queue process was stopped by the supervisor
            rabbit_misc:with_exit_handler(
              fun () -> retry_wait(Q, F, E, RetriesLeft) end,
              fun () -> F(Q) end);
        %% The queue is supposed to be active.
        %% The master node can go away or queue can be killed
        %% so we retry, waiting for a slave to take over.
        {ok, Q} when ?amqqueue_state_is(Q, live) ->
            %% We check is_process_alive(QPid) in case we receive a
            %% nodedown (for example) in F() that has nothing to do
            %% with the QPid. F() should be written s.t. that this
            %% cannot happen, so we bail if it does since that
            %% indicates a code bug and we don't want to get stuck in
            %% the retry loop.
            rabbit_misc:with_exit_handler(
              fun () -> retry_wait(Q, F, E, RetriesLeft) end,
              fun () -> F(Q) end);
        {error, not_found} ->
            E(not_found_or_absent_dirty(Name))
    end.

-spec retry_wait(amqqueue:amqqueue(),
                 qfun(A),
                 fun((not_found_or_absent()) -> rabbit_types:channel_exit()),
                 non_neg_integer()) ->
    A | rabbit_types:channel_exit().

retry_wait(Q, F, E, RetriesLeft) ->
    Name = amqqueue:get_name(Q),
    QPid = amqqueue:get_pid(Q),
    QState = amqqueue:get_state(Q),
    case {QState, is_replicated(Q)} of
        %% We don't want to repeat an operation if
        %% there are no slaves to migrate to
        {stopped, false} ->
            E({absent, Q, stopped});
        _ ->
            case rabbit_mnesia:is_process_alive(QPid) of
                true ->
                    % rabbitmq-server#1682
                    % The old check would have crashed here,
                    % instead, log it and run the exit fun. absent & alive is weird,
                    % but better than crashing with badmatch,true
                    rabbit_log:debug("Unexpected alive queue process ~p~n", [QPid]),
                    E({absent, Q, alive});
                false ->
                    ok % Expected result
            end,
            timer:sleep(30),
            with(Name, F, E, RetriesLeft - 1)
    end.

-spec with(name(), qfun(A)) ->
          A | rabbit_types:error(not_found_or_absent()).

with(Name, F) -> with(Name, F, fun (E) -> {error, E} end).

-spec with_or_die(name(), qfun(A)) -> A | rabbit_types:channel_exit().

with_or_die(Name, F) ->
    with(Name, F, die_fun(Name)).

-spec die_fun(name()) ->
    fun((not_found_or_absent()) -> rabbit_types:channel_exit()).

die_fun(Name) ->
    fun (not_found)           -> not_found(Name);
        ({absent, Q, Reason}) -> absent(Q, Reason)
    end.

-spec not_found(name()) -> rabbit_types:channel_exit().

not_found(R) -> rabbit_misc:protocol_error(not_found, "no ~s", [rabbit_misc:rs(R)]).

-spec absent(amqqueue:amqqueue(), absent_reason()) ->
    rabbit_types:channel_exit().

absent(Q, AbsentReason) ->
    QueueName = amqqueue:get_name(Q),
    QPid = amqqueue:get_pid(Q),
    IsDurable = amqqueue:is_durable(Q),
    priv_absent(QueueName, QPid, IsDurable, AbsentReason).

-spec priv_absent(name(), pid(), boolean(), absent_reason()) ->
    rabbit_types:channel_exit().

priv_absent(QueueName, QPid, true, nodedown) ->
    %% The assertion of durability is mainly there because we mention
    %% durability in the error message. That way we will hopefully
    %% notice if at some future point our logic changes s.t. we get
    %% here with non-durable queues.
    rabbit_misc:protocol_error(
      not_found,
      "home node '~s' of durable ~s is down or inaccessible",
      [node(QPid), rabbit_misc:rs(QueueName)]);

priv_absent(QueueName, _QPid, _IsDurable, stopped) ->
    rabbit_misc:protocol_error(
      not_found,
      "~s process is stopped by supervisor", [rabbit_misc:rs(QueueName)]);

priv_absent(QueueName, _QPid, _IsDurable, crashed) ->
    rabbit_misc:protocol_error(
      not_found,
      "~s has crashed and failed to restart", [rabbit_misc:rs(QueueName)]);

priv_absent(QueueName, _QPid, _IsDurable, timeout) ->
    rabbit_misc:protocol_error(
      not_found,
      "failed to perform operation on ~s due to timeout", [rabbit_misc:rs(QueueName)]).

-spec assert_equivalence
        (amqqueue:amqqueue(), boolean(), boolean(),
         rabbit_framing:amqp_table(), rabbit_types:maybe(pid())) ->
            'ok' | rabbit_types:channel_exit() | rabbit_types:connection_exit().

assert_equivalence(Q, DurableDeclare, AutoDeleteDeclare, Args1, Owner) ->
    QName = amqqueue:get_name(Q),
    DurableQ = amqqueue:is_durable(Q),
    AutoDeleteQ = amqqueue:is_auto_delete(Q),
    ok = check_exclusive_access(Q, Owner, strict),
    ok = rabbit_misc:assert_field_equivalence(DurableQ, DurableDeclare, QName, durable),
    ok = rabbit_misc:assert_field_equivalence(AutoDeleteQ, AutoDeleteDeclare, QName, auto_delete),
    ok = assert_args_equivalence(Q, Args1).

-spec check_exclusive_access(amqqueue:amqqueue(), pid()) ->
          'ok' | rabbit_types:channel_exit().

check_exclusive_access(Q, Owner) -> check_exclusive_access(Q, Owner, lax).

check_exclusive_access(Q, Owner, _MatchType)
  when ?amqqueue_exclusive_owner_is(Q, Owner) ->
    ok;
check_exclusive_access(Q, _ReaderPid, lax)
  when ?amqqueue_exclusive_owner_is(Q, none) ->
    ok;
check_exclusive_access(Q, _ReaderPid, _MatchType) ->
    QueueName = amqqueue:get_name(Q),
    rabbit_misc:protocol_error(
      resource_locked,
      "cannot obtain exclusive access to locked ~s. It could be originally "
      "declared on another connection or the exclusive property value does not "
      "match that of the original declaration.",
      [rabbit_misc:rs(QueueName)]).

-spec with_exclusive_access_or_die(name(), pid(), qfun(A)) ->
          A | rabbit_types:channel_exit().

with_exclusive_access_or_die(Name, ReaderPid, F) ->
    with_or_die(Name,
                fun (Q) -> check_exclusive_access(Q, ReaderPid), F(Q) end).

assert_args_equivalence(Q, RequiredArgs) ->
    QueueName = amqqueue:get_name(Q),
    Args = amqqueue:get_arguments(Q),
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
     {<<"x-dead-letter-exchange">>,    fun check_dlxname_arg/2},
     {<<"x-dead-letter-routing-key">>, fun check_dlxrk_arg/2},
     {<<"x-max-length">>,              fun check_non_neg_int_arg/2},
     {<<"x-max-length-bytes">>,        fun check_non_neg_int_arg/2},
     {<<"x-max-priority">>,            fun check_max_priority_arg/2},
     {<<"x-overflow">>,                fun check_overflow/2},
     {<<"x-queue-mode">>,              fun check_queue_mode/2},
     {<<"x-single-active-consumer">>,  fun check_single_active_consumer_arg/2},
     {<<"x-queue-type">>,              fun check_queue_type/2},
     {<<"x-quorum-initial-group-size">>,     fun check_default_quorum_initial_group_size_arg/2}].

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

check_max_priority_arg({Type, Val}, Args) ->
    case check_non_neg_int_arg({Type, Val}, Args) of
        ok when Val =< ?MAX_SUPPORTED_PRIORITY -> ok;
        ok                                     -> {error, {max_value_exceeded, Val}};
        Error                                  -> Error
    end.

check_single_active_consumer_arg({Type, Val}, Args) ->
    case check_bool_arg({Type, Val}, Args) of
        ok    -> ok;
        Error -> Error
    end.

check_default_quorum_initial_group_size_arg({Type, Val}, Args) ->
    case check_non_neg_int_arg({Type, Val}, Args) of
        ok when Val == 0 -> {error, {value_zero, Val}};
        ok               -> ok;
        Error            -> Error
    end.

%% Note that the validity of x-dead-letter-exchange is already verified
%% by rabbit_channel's queue.declare handler.
check_dlxname_arg({longstr, _}, _) -> ok;
check_dlxname_arg({Type,    _}, _) -> {error, {unacceptable_type, Type}}.

check_dlxrk_arg({longstr, _}, Args) ->
    case rabbit_misc:table_lookup(Args, <<"x-dead-letter-exchange">>) of
        undefined -> {error, routing_key_but_no_dlx_defined};
        _         -> ok
    end;
check_dlxrk_arg({Type,    _}, _Args) ->
    {error, {unacceptable_type, Type}}.

check_overflow({longstr, Val}, _Args) ->
    case lists:member(Val, [<<"drop-head">>, <<"reject-publish">>]) of
        true  -> ok;
        false -> {error, invalid_overflow}
    end;
check_overflow({Type,    _}, _Args) ->
    {error, {unacceptable_type, Type}}.

check_queue_mode({longstr, Val}, _Args) ->
    case lists:member(Val, [<<"default">>, <<"lazy">>]) of
        true  -> ok;
        false -> {error, invalid_queue_mode}
    end;
check_queue_mode({Type,    _}, _Args) ->
    {error, {unacceptable_type, Type}}.

check_queue_type({longstr, Val}, _Args) ->
    case lists:member(Val, [<<"classic">>, <<"quorum">>]) of
        true  -> ok;
        false -> {error, invalid_queue_type}
    end;
check_queue_type({Type,    _}, _Args) ->
    {error, {unacceptable_type, Type}}.

-spec list() -> [amqqueue:amqqueue()].

list() ->
    list_with_possible_retry(fun do_list/0).

do_list() ->
    mnesia:dirty_match_object(rabbit_queue, amqqueue:pattern_match_all()).

-spec list_names() -> [rabbit_amqqueue:name()].

list_names() -> mnesia:dirty_all_keys(rabbit_queue).

list_names(VHost) -> [amqqueue:get_name(Q) || Q <- list(VHost)].

list_local_names() ->
    [ amqqueue:get_name(Q) || Q <- list(),
           amqqueue:get_state(Q) =/= crashed, is_local_to_node(amqqueue:get_pid(Q), node())].

-spec list_by_type(atom()) -> [amqqueue:amqqueue()].

list_by_type(Type) ->
    {atomic, Qs} =
        mnesia:sync_transaction(
          fun () ->
                  mnesia:match_object(rabbit_durable_queue,
                                      amqqueue:pattern_match_on_type(Type),
                                      read)
          end),
    Qs.

list_local_followers() ->
    [ amqqueue:get_name(Q)
      || Q <- list(),
         amqqueue:is_quorum(Q),
         amqqueue:get_state(Q) =/= crashed, amqqueue:get_leader(Q) =/= node(), lists:member(node(), amqqueue:get_quorum_nodes(Q))].

is_local_to_node(QPid, Node) when ?IS_CLASSIC(QPid) ->
    Node =:= node(QPid);
is_local_to_node({_, Leader} = QPid, Node) when ?IS_QUORUM(QPid) ->
    Node =:= Leader.

-spec list(rabbit_types:vhost()) -> [amqqueue:amqqueue()].

list(VHostPath) ->
    list(VHostPath, rabbit_queue).

list(VHostPath, TableName) ->
    list_with_possible_retry(fun() -> do_list(VHostPath, TableName) end).

%% Not dirty_match_object since that would not be transactional when used in a
%% tx context
do_list(VHostPath, TableName) ->
    mnesia:async_dirty(
      fun () ->
              mnesia:match_object(
                TableName,
                amqqueue:pattern_match_on_name(rabbit_misc:r(VHostPath, queue)),
                read)
      end).

list_with_possible_retry(Fun) ->
    %% amqqueue migration:
    %% The `rabbit_queue` or `rabbit_durable_queue` tables
    %% might be migrated between the time we query the pattern
    %% (with the `amqqueue` module) and the time we call
    %% `mnesia:dirty_match_object()`. This would lead to an empty list
    %% (no object matching the now incorrect pattern), not a Mnesia
    %% error.
    %%
    %% So if the result is an empty list and the version of the
    %% `amqqueue` record changed in between, we retry the operation.
    %%
    %% However, we don't do this if inside a Mnesia transaction: we
    %% could end up with a live lock between this started transaction
    %% and the Mnesia table migration which is blocked (but the
    %% rabbit_feature_flags lock is held).
    AmqqueueRecordVersion = amqqueue:record_version_to_use(),
    case Fun() of
        [] ->
            case mnesia:is_transaction() of
                true ->
                    [];
                false ->
                    case amqqueue:record_version_to_use() of
                        AmqqueueRecordVersion -> [];
                        _                     -> Fun()
                    end
            end;
        Ret ->
            Ret
    end.

-spec list_down(rabbit_types:vhost()) -> [amqqueue:amqqueue()].

list_down(VHostPath) ->
    case rabbit_vhost:exists(VHostPath) of
        false -> [];
        true  ->
            Present = list(VHostPath),
            Durable = list(VHostPath, rabbit_durable_queue),
            PresentS = sets:from_list([amqqueue:get_name(Q) || Q <- Present]),
            sets:to_list(sets:filter(fun (Q) ->
                                             N = amqqueue:get_name(Q),
                                             not sets:is_element(N, PresentS)
                                     end, sets:from_list(Durable)))
    end.

count(VHost) ->
  try
    %% this is certainly suboptimal but there is no way to count
    %% things using a secondary index in Mnesia. Our counter-table-per-node
    %% won't work here because with master migration of mirrored queues
    %% the "ownership" of queues by nodes becomes a non-trivial problem
    %% that requires a proper consensus algorithm.
    length(list_for_count(VHost))
  catch _:Err ->
    rabbit_log:error("Failed to fetch number of queues in vhost ~p:~n~p~n",
                     [VHost, Err]),
    0
  end.

list_for_count(VHost) ->
    list_with_possible_retry(
      fun() ->
              mnesia:dirty_index_read(rabbit_queue,
                                      VHost,
                                      amqqueue:field_vhost())
      end).

-spec info_keys() -> rabbit_types:info_keys().

info_keys() -> rabbit_amqqueue_process:info_keys().

map(Qs, F) -> rabbit_misc:filter_exit_map(F, Qs).

is_unresponsive(Q, _Timeout) when ?amqqueue_state_is(Q, crashed) ->
    false;
is_unresponsive(Q, Timeout) ->
    QPid = amqqueue:get_pid(Q),
    try
        delegate:invoke(QPid, {gen_server2, call, [{info, [name]}, Timeout]}),
        false
    catch
        %% TODO catch any exit??
        exit:{timeout, _} ->
            true
    end.

format(Q) when ?amqqueue_is_quorum(Q) -> rabbit_quorum_queue:format(Q);
format(_) -> [].

-spec info(amqqueue:amqqueue()) -> rabbit_types:infos().

info(Q) when ?amqqueue_is_quorum(Q) -> rabbit_quorum_queue:info(Q);
info(Q) when ?amqqueue_state_is(Q, crashed) -> info_down(Q, crashed);
info(Q) when ?amqqueue_state_is(Q, stopped) -> info_down(Q, stopped);
info(Q) ->
    QPid = amqqueue:get_pid(Q),
    delegate:invoke(QPid, {gen_server2, call, [info, infinity]}).

-spec info(amqqueue:amqqueue(), rabbit_types:info_keys()) ->
          rabbit_types:infos().

info(Q, Items) when ?amqqueue_is_quorum(Q) ->
    rabbit_quorum_queue:info(Q, Items);
info(Q, Items) when ?amqqueue_state_is(Q, crashed) ->
    info_down(Q, Items, crashed);
info(Q, Items) when ?amqqueue_state_is(Q, stopped) ->
    info_down(Q, Items, stopped);
info(Q, Items) ->
    QPid = amqqueue:get_pid(Q),
    case delegate:invoke(QPid, {gen_server2, call, [{info, Items}, infinity]}) of
        {ok, Res}      -> Res;
        {error, Error} -> throw(Error)
    end.

info_down(Q, DownReason) ->
    info_down(Q, rabbit_amqqueue_process:info_keys(), DownReason).

info_down(Q, Items, DownReason) ->
    [{Item, i_down(Item, Q, DownReason)} || Item <- Items].

i_down(name,               Q, _) -> amqqueue:get_name(Q);
i_down(durable,            Q, _) -> amqqueue:is_durable(Q);
i_down(auto_delete,        Q, _) -> amqqueue:is_auto_delete(Q);
i_down(arguments,          Q, _) -> amqqueue:get_arguments(Q);
i_down(pid,                Q, _) -> amqqueue:get_pid(Q);
i_down(recoverable_slaves, Q, _) -> amqqueue:get_recoverable_slaves(Q);
i_down(state, _Q, DownReason)    -> DownReason;
i_down(K, _Q, _DownReason) ->
    case lists:member(K, rabbit_amqqueue_process:info_keys()) of
        true  -> '';
        false -> throw({bad_argument, K})
    end.

-spec info_all(rabbit_types:vhost()) -> [rabbit_types:infos()].

info_all(VHostPath) ->
    map(list(VHostPath), fun (Q) -> info(Q) end) ++
        map(list_down(VHostPath), fun (Q) -> info_down(Q, down) end).

-spec info_all(rabbit_types:vhost(), rabbit_types:info_keys()) ->
          [rabbit_types:infos()].

info_all(VHostPath, Items) ->
    map(list(VHostPath), fun (Q) -> info(Q, Items) end) ++
        map(list_down(VHostPath), fun (Q) -> info_down(Q, Items, down) end).

emit_info_local(VHostPath, Items, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map_with_exit_handler(
      AggregatorPid, Ref, fun(Q) -> info(Q, Items) end, list_local(VHostPath)).

emit_info_all(Nodes, VHostPath, Items, Ref, AggregatorPid) ->
    Pids = [ spawn_link(Node, rabbit_amqqueue, emit_info_local, [VHostPath, Items, Ref, AggregatorPid]) || Node <- Nodes ],
    rabbit_control_misc:await_emitters_termination(Pids).

emit_info_down(VHostPath, Items, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map_with_exit_handler(
      AggregatorPid, Ref, fun(Q) -> info_down(Q, Items, down) end,
      list_down(VHostPath)).

emit_unresponsive_local(VHostPath, Items, Timeout, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map_with_exit_handler(
      AggregatorPid, Ref, fun(Q) -> case is_unresponsive(Q, Timeout) of
                                        true -> info_down(Q, Items, unresponsive);
                                        false -> []
                                    end
                          end, list_local(VHostPath)
     ).

emit_unresponsive(Nodes, VHostPath, Items, Timeout, Ref, AggregatorPid) ->
    Pids = [ spawn_link(Node, rabbit_amqqueue, emit_unresponsive_local,
                        [VHostPath, Items, Timeout, Ref, AggregatorPid]) || Node <- Nodes ],
    rabbit_control_misc:await_emitters_termination(Pids).

info_local(VHostPath) ->
    map(list_local(VHostPath), fun (Q) -> info(Q, [name]) end).

list_local(VHostPath) ->
    [Q || Q <- list(VHostPath),
          amqqueue:get_state(Q) =/= crashed, is_local_to_node(amqqueue:get_pid(Q), node())].

-spec force_event_refresh(reference()) -> 'ok'.

force_event_refresh(Ref) ->
    [gen_server2:cast(amqqueue:get_pid(Q),
                      {force_event_refresh, Ref}) || Q <- list()],
    ok.

-spec notify_policy_changed(amqqueue:amqqueue()) -> 'ok'.

notify_policy_changed(Q) when ?amqqueue_is_classic(Q) ->
    QPid = amqqueue:get_pid(Q),
    gen_server2:cast(QPid, policy_changed);
notify_policy_changed(Q) when ?amqqueue_is_quorum(Q) ->
    QPid = amqqueue:get_pid(Q),
    QName = amqqueue:get_name(Q),
    rabbit_quorum_queue:policy_changed(QName, QPid).

-spec consumers(amqqueue:amqqueue()) ->
          [{pid(), rabbit_types:ctag(), boolean(), non_neg_integer(),
            boolean(), atom(),
            rabbit_framing:amqp_table(), rabbit_types:username()}].

consumers(Q) when ?amqqueue_is_classic(Q) ->
    QPid = amqqueue:get_pid(Q),
    delegate:invoke(QPid, {gen_server2, call, [consumers, infinity]});
consumers(Q) when ?amqqueue_is_quorum(Q) ->
    QPid = amqqueue:get_pid(Q),
    {ok, {_, Result}, _} = ra:local_query(QPid,
                                          fun rabbit_fifo:query_consumers/1),
    maps:values(Result).

-spec consumer_info_keys() -> rabbit_types:info_keys().

consumer_info_keys() -> ?CONSUMER_INFO_KEYS.

-spec consumers_all(rabbit_types:vhost()) ->
          [{name(), pid(), rabbit_types:ctag(), boolean(),
            non_neg_integer(), rabbit_framing:amqp_table()}].

consumers_all(VHostPath) ->
    ConsumerInfoKeys = consumer_info_keys(),
    lists:append(
      map(list(VHostPath),
          fun(Q) -> get_queue_consumer_info(Q, ConsumerInfoKeys) end)).

emit_consumers_all(Nodes, VHostPath, Ref, AggregatorPid) ->
    Pids = [ spawn_link(Node, rabbit_amqqueue, emit_consumers_local, [VHostPath, Ref, AggregatorPid]) || Node <- Nodes ],
    rabbit_control_misc:await_emitters_termination(Pids),
    ok.

emit_consumers_local(VHostPath, Ref, AggregatorPid) ->
    ConsumerInfoKeys = consumer_info_keys(),
    rabbit_control_misc:emitting_map(
      AggregatorPid, Ref,
      fun(Q) -> get_queue_consumer_info(Q, ConsumerInfoKeys) end,
      list_local(VHostPath)).

get_queue_consumer_info(Q, ConsumerInfoKeys) ->
    [lists:zip(ConsumerInfoKeys,
               [amqqueue:get_name(Q), ChPid, CTag,
                AckRequired, Prefetch, Active, ActivityStatus, Args]) ||
        {ChPid, CTag, AckRequired, Prefetch, Active, ActivityStatus, Args, _} <- consumers(Q)].

-spec stat(amqqueue:amqqueue()) ->
          {'ok', non_neg_integer(), non_neg_integer()}.

stat(Q) when ?amqqueue_is_quorum(Q) -> rabbit_quorum_queue:stat(Q);
stat(Q) -> delegate:invoke(amqqueue:get_pid(Q), {gen_server2, call, [stat, infinity]}).

-spec pid_of(amqqueue:amqqueue()) ->
          {'ok', pid()} | rabbit_types:error('not_found').

pid_of(Q) -> amqqueue:get_pid(Q).

-spec pid_of(rabbit_types:vhost(), rabbit_misc:resource_name()) ->
          {'ok', pid()} | rabbit_types:error('not_found').

pid_of(VHost, QueueName) ->
  case lookup(rabbit_misc:r(VHost, queue, QueueName)) of
    {ok, Q}                -> pid_of(Q);
    {error, not_found} = E -> E
  end.

-spec delete_exclusive(qpids(), pid()) -> 'ok'.

delete_exclusive(QPids, ConnId) ->
    [gen_server2:cast(QPid, {delete_exclusive, ConnId}) || QPid <- QPids],
    ok.

-spec delete_immediately(qpids()) -> 'ok'.

delete_immediately(QPids) ->
    {Classic, Quorum} = filter_pid_per_type(QPids),
    [gen_server2:cast(QPid, delete_immediately) || QPid <- Classic],
    case Quorum of
        [] -> ok;
        _ -> {error, cannot_delete_quorum_queues, Quorum}
    end.

delete_immediately_by_resource(Resources) ->
    {Classic, Quorum} = filter_resource_per_type(Resources),
    [gen_server2:cast(QPid, delete_immediately) || {_, QPid} <- Classic],
    [rabbit_quorum_queue:delete_immediately(Resource, QPid)
     || {Resource, QPid} <- Quorum],
    ok.

-spec delete
        (amqqueue:amqqueue(), 'false', 'false', rabbit_types:username()) ->
            qlen();
        (amqqueue:amqqueue(), 'true' , 'false', rabbit_types:username()) ->
            qlen() | rabbit_types:error('in_use');
        (amqqueue:amqqueue(), 'false', 'true', rabbit_types:username()) ->
            qlen() | rabbit_types:error('not_empty');
        (amqqueue:amqqueue(), 'true' , 'true', rabbit_types:username()) ->
            qlen() |
            rabbit_types:error('in_use') |
            rabbit_types:error('not_empty').

delete(Q,
       IfUnused, IfEmpty, ActingUser) when ?amqqueue_is_quorum(Q) ->
    rabbit_quorum_queue:delete(Q, IfUnused, IfEmpty, ActingUser);
delete(Q, IfUnused, IfEmpty, ActingUser) ->
    case wait_for_promoted_or_stopped(Q) of
        {promoted, Q1} ->
            QPid = amqqueue:get_pid(Q1),
            delegate:invoke(QPid, {gen_server2, call, [{delete, IfUnused, IfEmpty, ActingUser}, infinity]});
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

-spec wait_for_promoted_or_stopped(amqqueue:amqqueue()) ->
    {promoted, amqqueue:amqqueue()} |
    {stopped, amqqueue:amqqueue()} |
    {error, not_found}.
wait_for_promoted_or_stopped(Q0) ->
    QName = amqqueue:get_name(Q0),
    case lookup(QName) of
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
    ok = rpc:call(amqqueue:qnode(Q), ?MODULE, delete_crashed_internal, [Q, ActingUser]).

-spec delete_crashed_internal(amqqueue:amqqueue(), rabbit_types:username()) -> 'ok'.

delete_crashed_internal(Q, ActingUser) ->
    QName = amqqueue:get_name(Q),
    {ok, BQ} = application:get_env(rabbit, backing_queue_module),
    BQ:delete_crashed(Q),
    ok = internal_delete(QName, ActingUser).

-spec purge(amqqueue:amqqueue()) -> {ok, qlen()}.

purge(Q) when ?amqqueue_is_classic(Q) ->
    QPid = amqqueue:get_pid(Q),
    delegate:invoke(QPid, {gen_server2, call, [purge, infinity]});
purge(Q) when ?amqqueue_is_quorum(Q) ->
    NodeId = amqqueue:get_pid(Q),
    rabbit_quorum_queue:purge(NodeId).

-spec requeue(pid(),
              {rabbit_fifo:consumer_tag(), [msg_id()]},
              pid(),
              quorum_states()) ->
    'ok'.

requeue(QPid, {_, MsgIds}, ChPid, QuorumStates) when ?IS_CLASSIC(QPid) ->
    ok = delegate:invoke(QPid, {gen_server2, call, [{requeue, MsgIds, ChPid}, infinity]}),
    QuorumStates;
requeue({Name, _} = QPid, {CTag, MsgIds}, _ChPid, QuorumStates)
  when ?IS_QUORUM(QPid) ->
    case QuorumStates of
        #{Name := QState0} ->
            {ok, QState} = rabbit_quorum_queue:requeue(CTag, MsgIds, QState0),
            maps:put(Name, QState, QuorumStates);
        _ ->
            % queue was not found
            QuorumStates
    end.

-spec ack(pid(),
          {rabbit_fifo:consumer_tag(), [msg_id()]},
          pid(),
          quorum_states()) ->
    quorum_states().

ack(QPid, {_, MsgIds}, ChPid, QueueStates) when ?IS_CLASSIC(QPid) ->
    delegate:invoke_no_result(QPid, {gen_server2, cast, [{ack, MsgIds, ChPid}]}),
    QueueStates;
ack({Name, _} = QPid, {CTag, MsgIds}, _ChPid, QuorumStates)
  when ?IS_QUORUM(QPid) ->
    case QuorumStates of
        #{Name := QState0} ->
            {ok, QState} = rabbit_quorum_queue:ack(CTag, MsgIds, QState0),
            maps:put(Name, QState, QuorumStates);
        _ ->
            %% queue was not found
            QuorumStates
    end.

-spec reject(pid() | amqqueue:ra_server_id(),
             boolean(),
             {rabbit_fifo:consumer_tag(), [msg_id()]},
             pid(),
             quorum_states()) ->
    quorum_states().

reject(QPid, Requeue, {_, MsgIds}, ChPid, QStates) when ?IS_CLASSIC(QPid) ->
    ok = delegate:invoke_no_result(QPid, {gen_server2, cast,
                                          [{reject, Requeue, MsgIds, ChPid}]}),
    QStates;
reject({Name, _} = QPid, Requeue, {CTag, MsgIds}, _ChPid, QuorumStates)
  when ?IS_QUORUM(QPid) ->
    case QuorumStates of
        #{Name := QState0} ->
            {ok, QState} = rabbit_quorum_queue:reject(Requeue, CTag,
                                                      MsgIds, QState0),
            maps:put(Name, QState, QuorumStates);
        _ ->
            %% queue was not found
            QuorumStates
    end.

-spec notify_down_all(qpids(), pid()) -> ok_or_errors().

notify_down_all(QPids, ChPid) ->
    notify_down_all(QPids, ChPid, ?CHANNEL_OPERATION_TIMEOUT).

-spec notify_down_all(qpids(), pid(), non_neg_integer()) ->
          ok_or_errors().

notify_down_all(QPids, ChPid, Timeout) ->
    case rpc:call(node(), delegate, invoke,
                  [QPids, {gen_server2, call, [{notify_down, ChPid}, infinity]}], Timeout) of
        {badrpc, timeout} -> {error, {channel_operation_timeout, Timeout}};
        {badrpc, Reason}  -> {error, Reason};
        {_, Bads} ->
            case lists:filter(
                   fun ({_Pid, {exit, {R, _}, _}}) ->
                           rabbit_misc:is_abnormal_exit(R);
                       ({_Pid, _})                 -> false
                   end, Bads) of
                []    -> ok;
                Bads1 -> {error, Bads1}
            end;
        Error         -> {error, Error}
    end.

-spec activate_limit_all(qpids(), pid()) -> ok.

activate_limit_all(QRefs, ChPid) ->
    QPids = [P || P <- QRefs, ?IS_CLASSIC(P)],
    delegate:invoke_no_result(QPids, {gen_server2, cast,
                                      [{activate_limit, ChPid}]}).

-spec credit(amqqueue:amqqueue(),
             pid(),
             rabbit_types:ctag(),
             non_neg_integer(),
             boolean(),
             quorum_states()) ->
    {'ok', quorum_states()}.

credit(Q, ChPid, CTag, Credit,
       Drain, QStates) when ?amqqueue_is_classic(Q) ->
    QPid = amqqueue:get_pid(Q),
    delegate:invoke_no_result(QPid, {gen_server2, cast,
                                     [{credit, ChPid, CTag, Credit, Drain}]}),
    {ok, QStates};
credit(Q,
       _ChPid, CTag, Credit,
       Drain, QStates) when ?amqqueue_is_quorum(Q) ->
    {Name, _} = Id = amqqueue:get_pid(Q),
    QName = amqqueue:get_name(Q),
    QState0 = get_quorum_state(Id, QName, QStates),
    {ok, QState} = rabbit_quorum_queue:credit(CTag, Credit, Drain, QState0),
    {ok, maps:put(Name, QState, QStates)}.

-spec basic_get(amqqueue:amqqueue(), pid(), boolean(), pid(), rabbit_types:ctag(),
                #{Name :: atom() => rabbit_fifo_client:state()}) ->
          {'ok', non_neg_integer(), qmsg(), quorum_states()} |
          {'empty', quorum_states()} |
          rabbit_types:channel_exit().

basic_get(Q, ChPid, NoAck, LimiterPid, _CTag, _)
  when ?amqqueue_is_classic(Q) ->
    QPid = amqqueue:get_pid(Q),
    delegate:invoke(QPid, {gen_server2, call,
                           [{basic_get, ChPid, NoAck, LimiterPid}, infinity]});
basic_get(Q, _ChPid, NoAck, _LimiterPid, CTag, QStates)
  when ?amqqueue_is_quorum(Q) ->
    {Name, _} = Id = amqqueue:get_pid(Q),
    QName = amqqueue:get_name(Q),
    QState0 = get_quorum_state(Id, QName, QStates),
    case rabbit_quorum_queue:basic_get(Q, NoAck, CTag, QState0) of
        {ok, empty, QState} ->
            {empty, maps:put(Name, QState, QStates)};
        {ok, Count, Msg, QState} ->
            {ok, Count, Msg, maps:put(Name, QState, QStates)};
        {error, Reason} ->
            rabbit_misc:protocol_error(internal_error,
                                       "Cannot get a message from quorum queue '~s': ~p",
                                       [rabbit_misc:rs(QName), Reason])
    end.

-spec basic_consume
        (amqqueue:amqqueue(), boolean(), pid(), pid(), boolean(),
         non_neg_integer(), rabbit_types:ctag(), boolean(),
         rabbit_framing:amqp_table(), any(), rabbit_types:username(),
         #{Name :: atom() => rabbit_fifo_client:state()}) ->
            rabbit_types:ok_or_error('exclusive_consume_unavailable').

basic_consume(Q, NoAck, ChPid, LimiterPid,
              LimiterActive, ConsumerPrefetchCount, ConsumerTag,
              ExclusiveConsume, Args, OkMsg, ActingUser, QState)
  when ?amqqueue_is_classic(Q) ->
    QPid = amqqueue:get_pid(Q),
    QName = amqqueue:get_name(Q),
    ok = check_consume_arguments(QName, Args),
    case delegate:invoke(QPid,
                         {gen_server2, call,
                          [{basic_consume, NoAck, ChPid, LimiterPid, LimiterActive,
                            ConsumerPrefetchCount, ConsumerTag, ExclusiveConsume,
                            Args, OkMsg, ActingUser}, infinity]}) of
        ok ->
            {ok, QState};
        Err ->
            Err
    end;
basic_consume(Q, _NoAck, _ChPid,
              _LimiterPid, true, _ConsumerPrefetchCount, _ConsumerTag,
              _ExclusiveConsume, _Args, _OkMsg, _ActingUser, _QStates)
  when ?amqqueue_is_quorum(Q) ->
    {error, global_qos_not_supported_for_queue_type};
basic_consume(Q,
              NoAck, ChPid, _LimiterPid, _LimiterActive, ConsumerPrefetchCount,
              ConsumerTag, ExclusiveConsume, Args, OkMsg,
              ActingUser, QStates)
  when ?amqqueue_is_quorum(Q) ->
    {Name, _} = Id = amqqueue:get_pid(Q),
    QName = amqqueue:get_name(Q),
    ok = check_consume_arguments(QName, Args),
    QState0 = get_quorum_state(Id, QName, QStates),
    {ok, QState} = rabbit_quorum_queue:basic_consume(Q, NoAck, ChPid,
                                                     ConsumerPrefetchCount,
                                                     ConsumerTag,
                                                     ExclusiveConsume, Args,
                                                     ActingUser,
                                                     OkMsg, QState0),
    {ok, maps:put(Name, QState, QStates)}.

-spec basic_cancel
        (amqqueue:amqqueue(), pid(), rabbit_types:ctag(), any(),
         rabbit_types:username(), #{Name :: atom() => rabbit_fifo_client:state()}) ->
                          'ok' | {'ok', #{Name :: atom() => rabbit_fifo_client:state()}}.

basic_cancel(Q, ChPid, ConsumerTag, OkMsg, ActingUser,
             QState)
  when ?amqqueue_is_classic(Q) ->
    QPid = amqqueue:get_pid(Q),
    case delegate:invoke(QPid, {gen_server2, call,
                                [{basic_cancel, ChPid, ConsumerTag, OkMsg, ActingUser},
                                 infinity]}) of
        ok ->
            {ok, QState};
        Err -> Err
    end;
basic_cancel(Q, ChPid,
             ConsumerTag, OkMsg, _ActingUser, QStates)
  when ?amqqueue_is_quorum(Q) ->
    {Name, _} = Id = amqqueue:get_pid(Q),
    QState0 = get_quorum_state(Id, QStates),
    {ok, QState} = rabbit_quorum_queue:basic_cancel(ConsumerTag, ChPid, OkMsg, QState0),
    {ok, maps:put(Name, QState, QStates)}.

-spec notify_decorators(amqqueue:amqqueue()) -> 'ok'.

notify_decorators(Q) ->
    QPid = amqqueue:get_pid(Q),
    delegate:invoke_no_result(QPid, {gen_server2, cast, [notify_decorators]}).

notify_sent(QPid, ChPid) ->
    rabbit_amqqueue_common:notify_sent(QPid, ChPid).

notify_sent_queue_down(QPid) ->
    rabbit_amqqueue_common:notify_sent_queue_down(QPid).

-spec resume(pid(), pid()) -> 'ok'.

resume(QPid, ChPid) -> delegate:invoke_no_result(QPid, {gen_server2, cast, [{resume, ChPid}]}).

internal_delete1(QueueName, OnlyDurable) ->
    internal_delete1(QueueName, OnlyDurable, normal).

internal_delete1(QueueName, OnlyDurable, Reason) ->
    ok = mnesia:delete({rabbit_queue, QueueName}),
    case Reason of
        auto_delete ->
            case mnesia:wread({rabbit_durable_queue, QueueName}) of
                []  -> ok;
                [_] -> ok = mnesia:delete({rabbit_durable_queue, QueueName})
            end;
        _ ->
            mnesia:delete({rabbit_durable_queue, QueueName})
    end,
    %% we want to execute some things, as decided by rabbit_exchange,
    %% after the transaction.
    rabbit_binding:remove_for_destination(QueueName, OnlyDurable).

-spec internal_delete(name(), rabbit_types:username()) -> 'ok'.

internal_delete(QueueName, ActingUser) ->
    internal_delete(QueueName, ActingUser, normal).

internal_delete(QueueName, ActingUser, Reason) ->
    rabbit_misc:execute_mnesia_tx_with_tail(
      fun () ->
              case {mnesia:wread({rabbit_queue, QueueName}),
                    mnesia:wread({rabbit_durable_queue, QueueName})} of
                  {[], []} ->
                      rabbit_misc:const(ok);
                  _ ->
                      Deletions = internal_delete1(QueueName, false, Reason),
                      T = rabbit_binding:process_deletions(Deletions,
                                                           ?INTERNAL_USER),
                      fun() ->
                              ok = T(),
                              rabbit_core_metrics:queue_deleted(QueueName),
                              ok = rabbit_event:notify(queue_deleted,
                                                       [{name, QueueName},
                                                        {user_who_performed_action, ActingUser}])
                      end
              end
      end).

-spec forget_all_durable(node()) -> 'ok'.

forget_all_durable(Node) ->
    %% Note rabbit is not running so we avoid e.g. the worker pool. Also why
    %% we don't invoke the return from rabbit_binding:process_deletions/1.
    {atomic, ok} =
        mnesia:sync_transaction(
          fun () ->
                  Qs = mnesia:match_object(rabbit_durable_queue,
                                           amqqueue:pattern_match_all(), write),
                  [forget_node_for_queue(Node, Q) ||
                      Q <- Qs,
                      is_local_to_node(amqqueue:get_pid(Q), Node)],
                  ok
          end),
    ok.

%% Try to promote a slave while down - it should recover as a
%% master. We try to take the oldest slave here for best chance of
%% recovery.
forget_node_for_queue(DeadNode, Q)
  when ?amqqueue_is_quorum(Q) ->
    QN = amqqueue:get_quorum_nodes(Q),
    forget_node_for_queue(DeadNode, QN, Q);
forget_node_for_queue(DeadNode, Q) ->
    RS = amqqueue:get_recoverable_slaves(Q),
    forget_node_for_queue(DeadNode, RS, Q).

forget_node_for_queue(_DeadNode, [], Q) ->
    %% No slaves to recover from, queue is gone.
    %% Don't process_deletions since that just calls callbacks and we
    %% are not really up.
    Name = amqqueue:get_name(Q),
    internal_delete1(Name, true);

%% Should not happen, but let's be conservative.
forget_node_for_queue(DeadNode, [DeadNode | T], Q) ->
    forget_node_for_queue(DeadNode, T, Q);

forget_node_for_queue(DeadNode, [H|T], Q) when ?is_amqqueue(Q) ->
    Type = amqqueue:get_type(Q),
    case {node_permits_offline_promotion(H), Type} of
        {false, _} -> forget_node_for_queue(DeadNode, T, Q);
        {true, classic} -> Q1 = amqqueue:set_pid(Q, rabbit_misc:node_to_fake_pid(H)),
                           ok = mnesia:write(rabbit_durable_queue, Q1, write);
        {true, quorum} -> ok
    end.

node_permits_offline_promotion(Node) ->
    case node() of
        Node -> not rabbit:is_running(); %% [1]
        _    -> All = rabbit_mnesia:cluster_nodes(all),
                Running = rabbit_mnesia:cluster_nodes(running),
                lists:member(Node, All) andalso
                    not lists:member(Node, Running) %% [2]
    end.
%% [1] In this case if we are a real running node (i.e. rabbitmqctl
%% has RPCed into us) then we cannot allow promotion. If on the other
%% hand we *are* rabbitmqctl impersonating the node for offline
%% node-forgetting then we can.
%%
%% [2] This is simpler; as long as it's down that's OK

-spec run_backing_queue
        (pid(), atom(), (fun ((atom(), A) -> {[rabbit_types:msg_id()], A}))) ->
            'ok'.

run_backing_queue(QPid, Mod, Fun) ->
    gen_server2:cast(QPid, {run_backing_queue, Mod, Fun}).

-spec set_ram_duration_target(pid(), number() | 'infinity') -> 'ok'.

set_ram_duration_target(QPid, Duration) ->
    gen_server2:cast(QPid, {set_ram_duration_target, Duration}).

-spec set_maximum_since_use(pid(), non_neg_integer()) -> 'ok'.

set_maximum_since_use(QPid, Age) ->
    gen_server2:cast(QPid, {set_maximum_since_use, Age}).

-spec update_mirroring(pid()) -> 'ok'.

update_mirroring(QPid) ->
    ok = delegate:invoke_no_result(QPid, {gen_server2, cast, [update_mirroring]}).

-spec sync_mirrors(amqqueue:amqqueue() | pid()) ->
          'ok' | rabbit_types:error('not_mirrored').

sync_mirrors(Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    delegate:invoke(QPid, {gen_server2, call, [sync_mirrors, infinity]});
sync_mirrors(QPid) ->
    delegate:invoke(QPid, {gen_server2, call, [sync_mirrors, infinity]}).

-spec cancel_sync_mirrors(amqqueue:amqqueue() | pid()) ->
          'ok' | {'ok', 'not_syncing'}.

cancel_sync_mirrors(Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    delegate:invoke(QPid, {gen_server2, call, [cancel_sync_mirrors, infinity]});
cancel_sync_mirrors(QPid) ->
    delegate:invoke(QPid, {gen_server2, call, [cancel_sync_mirrors, infinity]}).

-spec is_replicated(amqqueue:amqqueue()) -> boolean().

is_replicated(Q) when ?amqqueue_is_quorum(Q) ->
    true;
is_replicated(Q) ->
    rabbit_mirror_queue_misc:is_mirrored(Q).

is_dead_exclusive(Q) when ?amqqueue_exclusive_owner_is(Q, none) ->
    false;
is_dead_exclusive(Q) when ?amqqueue_exclusive_owner_is_pid(Q) ->
    Pid = amqqueue:get_pid(Q),
    not rabbit_mnesia:is_process_alive(Pid).

-spec on_node_up(node()) -> 'ok'.

on_node_up(Node) ->
    ok = rabbit_misc:execute_mnesia_transaction(
           fun () ->
                   Qs = mnesia:match_object(rabbit_queue,
                                            amqqueue:pattern_match_all(), write),
                   [maybe_clear_recoverable_node(Node, Q) || Q <- Qs],
                   ok
           end).

maybe_clear_recoverable_node(Node, Q) ->
    SPids = amqqueue:get_sync_slave_pids(Q),
    RSs = amqqueue:get_recoverable_slaves(Q),
    case lists:member(Node, RSs) of
        true  ->
            %% There is a race with
            %% rabbit_mirror_queue_slave:record_synchronised/1 called
            %% by the incoming slave node and this function, called
            %% by the master node. If this function is executed after
            %% record_synchronised/1, the node is erroneously removed
            %% from the recoverable slaves list.
            %%
            %% We check if the slave node's queue PID is alive. If it is
            %% the case, then this function is executed after. In this
            %% situation, we don't touch the queue record, it is already
            %% correct.
            DoClearNode =
                case [SP || SP <- SPids, node(SP) =:= Node] of
                    [SPid] -> not rabbit_misc:is_process_alive(SPid);
                    _      -> true
                end,
            if
                DoClearNode -> RSs1 = RSs -- [Node],
                               store_queue(
                                 amqqueue:set_recoverable_slaves(Q, RSs1));
                true        -> ok
            end;
        false ->
            ok
    end.

-spec on_node_down(node()) -> 'ok'.

on_node_down(Node) ->
    {QueueNames, QueueDeletions} = delete_queues_on_node_down(Node),
    notify_queue_binding_deletions(QueueDeletions),
    rabbit_core_metrics:queues_deleted(QueueNames),
    notify_queues_deleted(QueueNames),
    ok.

delete_queues_on_node_down(Node) ->
    lists:unzip(lists:flatten([
        rabbit_misc:execute_mnesia_transaction(
          fun () -> [{Queue, delete_queue(Queue)} || Queue <- Queues] end
        ) || Queues <- partition_queues(queues_to_delete_when_node_down(Node))
    ])).

delete_queue(QueueName) ->
    ok = mnesia:delete({rabbit_queue, QueueName}),
    rabbit_binding:remove_transient_for_destination(QueueName).

% If there are many queues and we delete them all in a single Mnesia transaction,
% this can block all other Mnesia operations for a really long time.
% In situations where a node wants to (re-)join a cluster,
% Mnesia won't be able to sync on the new node until this operation finishes.
% As a result, we want to have multiple Mnesia transactions so that other
% operations can make progress in between these queue delete transactions.
%
% 10 queues per Mnesia transaction is an arbitrary number, but it seems to work OK with 50k queues per node.
partition_queues([Q0,Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9 | T]) ->
    [[Q0,Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9] | partition_queues(T)];
partition_queues(T) ->
    [T].

queues_to_delete_when_node_down(NodeDown) ->
    rabbit_misc:execute_mnesia_transaction(fun () ->
        qlc:e(qlc:q([amqqueue:get_name(Q) ||
            Q <- mnesia:table(rabbit_queue),
                amqqueue:qnode(Q) == NodeDown andalso
                not rabbit_mnesia:is_process_alive(amqqueue:get_pid(Q)) andalso
                (not rabbit_amqqueue:is_replicated(Q) orelse
                rabbit_amqqueue:is_dead_exclusive(Q))]
        ))
    end).

notify_queue_binding_deletions(QueueDeletions) ->
    rabbit_misc:execute_mnesia_tx_with_tail(
        fun() ->
            rabbit_binding:process_deletions(
                lists:foldl(
                    fun rabbit_binding:combine_deletions/2,
                    rabbit_binding:new_deletions(),
                    QueueDeletions
                ),
                ?INTERNAL_USER
            )
        end
    ).

notify_queues_deleted(QueueDeletions) ->
    lists:foreach(
      fun(Queue) ->
              ok = rabbit_event:notify(queue_deleted,
                                       [{name, Queue},
                                        {user, ?INTERNAL_USER}])
      end,
      QueueDeletions).

-spec pseudo_queue(name(), pid()) -> amqqueue:amqqueue().

pseudo_queue(QueueName, Pid) ->
    pseudo_queue(QueueName, Pid, false).

-spec pseudo_queue(name(), pid(), boolean()) -> amqqueue:amqqueue().

pseudo_queue(#resource{kind = queue} = QueueName, Pid, Durable)
  when is_pid(Pid) andalso
       is_boolean(Durable) ->
    amqqueue:new(QueueName,
                 Pid,
                 Durable,
                 false,
                 none, % Owner,
                 [],
                 undefined, % VHost,
                 #{user => undefined}, % ActingUser
                 classic % Type
                ).

-spec immutable(amqqueue:amqqueue()) -> amqqueue:amqqueue().

immutable(Q) -> amqqueue:set_immutable(Q).

-spec deliver([amqqueue:amqqueue()], rabbit_types:delivery()) -> 'ok'.

deliver(Qs, Delivery) ->
    deliver(Qs, Delivery, untracked),
    ok.

-spec deliver([amqqueue:amqqueue()],
              rabbit_types:delivery(),
              quorum_states() | 'untracked') ->
    {qpids(),
     [{amqqueue:ra_server_id(), name()}],
     quorum_states()}.

deliver([], _Delivery, QueueState) ->
    %% /dev/null optimisation
    {[], [], QueueState};

deliver(Qs, Delivery = #delivery{flow = Flow,
                                 confirm = Confirm}, QueueState0) ->
    {Quorum, MPids, SPids} = qpids(Qs),
    QPids = MPids ++ SPids,
    %% We use up two credits to send to a slave since the message
    %% arrives at the slave from two directions. We will ack one when
    %% the slave receives the message direct from the channel, and the
    %% other when it receives it via GM.

    case Flow of
        %% Here we are tracking messages sent by the rabbit_channel
        %% process. We are accessing the rabbit_channel process
        %% dictionary.
        flow   -> [credit_flow:send(QPid) || QPid <- QPids],
                  [credit_flow:send(QPid) || QPid <- SPids];
        noflow -> ok
    end,

    %% We let slaves know that they were being addressed as slaves at
    %% the time - if they receive such a message from the channel
    %% after they have become master they should mark the message as
    %% 'delivered' since they do not know what the master may have
    %% done with it.
    MMsg = {deliver, Delivery, false},
    SMsg = {deliver, Delivery, true},
    delegate:invoke_no_result(MPids, {gen_server2, cast, [MMsg]}),
    delegate:invoke_no_result(SPids, {gen_server2, cast, [SMsg]}),
    QueueState =
        case QueueState0 of
            untracked ->
                lists:foreach(
                  fun({Pid, _QName}) ->
                          rabbit_quorum_queue:stateless_deliver(Pid, Delivery)
                  end, Quorum),
                untracked;
            _ ->
                lists:foldl(
                  fun({{Name, _} = Pid, QName}, QStates) ->
                          QState0 = get_quorum_state(Pid, QName, QStates),
                          case rabbit_quorum_queue:deliver(Confirm, Delivery,
                                                           QState0) of
                              {ok, QState} ->
                                  maps:put(Name, QState, QStates);
                              {slow, QState} ->
                                  maps:put(Name, QState, QStates)
                          end
                  end, QueueState0, Quorum)
        end,
    {QuorumPids, _} = lists:unzip(Quorum),
    {QPids, QuorumPids, QueueState}.

qpids([]) -> {[], [], []}; %% optimisation
qpids([Q]) when ?amqqueue_is_quorum(Q) ->
    QName = amqqueue:get_name(Q),
    {LocalName, LeaderNode} = amqqueue:get_pid(Q),
    {[{{LocalName, LeaderNode}, QName}], [], []}; %% opt
qpids([Q]) ->
    QPid = amqqueue:get_pid(Q),
    SPids = amqqueue:get_slave_pids(Q),
    {[], [QPid], SPids}; %% opt
qpids(Qs) ->
    {QuoPids, MPids, SPids} =
        lists:foldl(fun (Q,
                         {QuoPidAcc, MPidAcc, SPidAcc})
                          when ?amqqueue_is_quorum(Q) ->
                            QPid = amqqueue:get_pid(Q),
                            QName = amqqueue:get_name(Q),
                            {[{QPid, QName} | QuoPidAcc], MPidAcc, SPidAcc};
                        (Q,
                         {QuoPidAcc, MPidAcc, SPidAcc}) ->
                            QPid = amqqueue:get_pid(Q),
                            SPids = amqqueue:get_slave_pids(Q),
                            {QuoPidAcc, [QPid | MPidAcc], [SPids | SPidAcc]}
                    end, {[], [], []}, Qs),
    {QuoPids, MPids, lists:append(SPids)}.

get_quorum_state({Name, _} = Id, QName, Map) ->
    case maps:find(Name, Map) of
        {ok, S} -> S;
        error ->
            rabbit_quorum_queue:init_state(Id, QName)
    end.

get_quorum_state({Name, _}, Map) ->
    maps:get(Name, Map).
