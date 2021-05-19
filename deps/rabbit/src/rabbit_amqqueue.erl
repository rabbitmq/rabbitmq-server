%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqqueue).

-export([warn_file_limit/0]).
-export([recover/1, stop/1, start/1, declare/6, declare/7,
         delete_immediately/1, delete_exclusive/2, delete/4, purge/1,
         forget_all_durable/1]).
-export([pseudo_queue/2, pseudo_queue/3, immutable/1]).
-export([lookup/1, lookup_many/1, not_found_or_absent/1, not_found_or_absent_dirty/1,
         with/2, with/3, with_or_die/2,
         assert_equivalence/5,
         check_exclusive_access/2, with_exclusive_access_or_die/3,
         stat/1, deliver/2,
         requeue/3, ack/3, reject/4]).
-export([not_found/1, absent/2]).
-export([list/0, list/1, info_keys/0, info/1, info/2, info_all/1, info_all/2,
         emit_info_all/5, list_local/1, info_local/1,
         emit_info_local/4, emit_info_down/4]).
-export([count/0]).
-export([list_down/1, count/1, list_names/0, list_names/1, list_local_names/0,
         list_local_names_down/0, list_with_possible_retry/1]).
-export([list_by_type/1, sample_local_queues/0, sample_n_by_name/2, sample_n/2]).
-export([force_event_refresh/1, notify_policy_changed/1]).
-export([consumers/1, consumers_all/1,  emit_consumers_all/4, consumer_info_keys/0]).
-export([basic_get/5, basic_consume/12, basic_cancel/5, notify_decorators/1]).
-export([notify_sent/2, notify_sent_queue_down/1, resume/2]).
-export([notify_down_all/2, notify_down_all/3, activate_limit_all/2, credit/5]).
-export([on_node_up/1, on_node_down/1]).
-export([update/2, store_queue/1, update_decorators/1, policy_changed/2]).
-export([update_mirroring/1, sync_mirrors/1, cancel_sync_mirrors/1]).
-export([emit_unresponsive/6, emit_unresponsive_local/5, is_unresponsive/2]).
-export([has_synchronised_mirrors_online/1, is_match/2, is_in_virtual_host/2]).
-export([is_replicated/1, is_exclusive/1, is_not_exclusive/1, is_dead_exclusive/1]).
-export([list_local_quorum_queues/0, list_local_quorum_queue_names/0, list_local_stream_queues/0,
         list_local_mirrored_classic_queues/0, list_local_mirrored_classic_names/0,
         list_local_leaders/0, list_local_followers/0, get_quorum_nodes/1,
         list_local_mirrored_classic_without_synchronised_mirrors/0,
         list_local_mirrored_classic_without_synchronised_mirrors_for_cli/0,
         list_local_quorum_queues_with_name_matching/1,
         list_local_quorum_queues_with_name_matching/2]).
-export([ensure_rabbit_queue_record_is_initialized/1]).
-export([format/1]).
-export([delete_immediately_by_resource/1]).
-export([delete_crashed/1,
         delete_crashed/2,
         delete_crashed_internal/2]).

-export([pid_of/1, pid_of/2]).
-export([mark_local_durable_queues_stopped/1]).

-export([rebalance/3]).
-export([collect_info_all/2]).

-export([is_policy_applicable/2, declare_args/0, consume_args/0]).
-export([is_server_named_allowed/1]).

-export([check_max_age/1]).
-export([get_queue_type/1, get_resource_vhost_name/1, get_resource_name/1]).

%% internal
-export([internal_declare/2, internal_delete/2, run_backing_queue/3,
         set_ram_duration_target/2, set_maximum_since_use/2,
         emit_consumers_local/3, internal_delete/3]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include("amqqueue.hrl").

-define(INTEGER_ARG_TYPES, [byte, short, signedint, long,
                            unsignedbyte, unsignedshort, unsignedint]).

-define(IS_CLASSIC(QPid), is_pid(QPid)).
-define(IS_QUORUM(QPid), is_tuple(QPid)).
%%----------------------------------------------------------------------------

-export_type([name/0, qmsg/0, absent_reason/0]).

-type name() :: rabbit_types:r('queue').

-type qpids() :: [pid()].
-type qlen() :: rabbit_types:ok(non_neg_integer()).
-type qfun(A) :: fun ((amqqueue:amqqueue()) -> A | no_return()).
-type qmsg() :: {name(), pid() | {atom(), pid()}, msg_id(),
                 boolean(), rabbit_types:message()}.
-type msg_id() :: non_neg_integer().
-type ok_or_errors() ::
        'ok' | {'error', [{'error' | 'exit' | 'throw', any()}]}.
-type absent_reason() :: 'nodedown' | 'crashed' | stopped | timeout.
-type queue_not_found() :: not_found.
-type queue_absent() :: {'absent', amqqueue:amqqueue(), absent_reason()}.
-type not_found_or_absent() :: queue_not_found() | queue_absent().

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
              "Recovering ~p queues, available file handles: ~p. Please increase max open file handles limit to at least ~p!",
              [L, file_handle_cache:get_limit(), L]);
        false ->
            ok
    end.

-spec recover(rabbit_types:vhost()) ->
    {Recovered :: [amqqueue:amqqueue()],
     Failed :: [amqqueue:amqqueue()]}.
recover(VHost) ->
    AllDurable = find_local_durable_queues(VHost),
    rabbit_queue_type:recover(VHost, AllDurable).

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
    Qs = find_local_durable_queues(VHost),
    rabbit_misc:execute_mnesia_transaction(
        fun() ->
            [ store_queue(amqqueue:set_state(Q, stopped))
              || Q <- Qs, amqqueue:get_type(Q) =:= rabbit_classic_queue,
                 amqqueue:get_state(Q) =/= stopped ]
        end).

find_local_durable_queues(VHost) ->
    mnesia:async_dirty(
      fun () ->
              qlc:e(
                qlc:q(
                  [Q || Q <- mnesia:table(rabbit_durable_queue),
                        amqqueue:get_vhost(Q) =:= VHost andalso
                        rabbit_queue_type:is_recoverable(Q)
                  ]))
      end).

find_recoverable_queues() ->
    mnesia:async_dirty(
      fun () ->
              qlc:e(qlc:q([Q || Q <- mnesia:table(rabbit_durable_queue),
                                rabbit_queue_type:is_recoverable(Q)]))
      end).

-spec declare(name(),
              boolean(),
              boolean(),
              rabbit_framing:amqp_table(),
              rabbit_types:maybe(pid()),
              rabbit_types:username()) ->
    {'new' | 'existing' | 'owner_died', amqqueue:amqqueue()} |
    {'new', amqqueue:amqqueue(), rabbit_fifo_client:state()} |
    {'absent', amqqueue:amqqueue(), absent_reason()} |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.
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
    {'absent', amqqueue:amqqueue(), absent_reason()} |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.
declare(QueueName = #resource{virtual_host = VHost}, Durable, AutoDelete, Args,
        Owner, ActingUser, Node) ->
    ok = check_declare_arguments(QueueName, Args),
    Type = get_queue_type(Args),
    case rabbit_queue_type:is_enabled(Type) of
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
            rabbit_queue_type:declare(Q, Node);
        false ->
            {protocol_error, internal_error,
             "Cannot declare a queue '~s' of type '~s' on node '~s': "
             "the corresponding feature flag is disabled",
              [rabbit_misc:rs(QueueName), Type, Node]}
    end.

get_queue_type(Args) ->
    case rabbit_misc:table_lookup(Args, <<"x-queue-type">>) of
        undefined ->
            rabbit_queue_type:default();
        {_, V} ->
            rabbit_queue_type:discover(V)
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
    notify_policy_changed(Q2).

is_policy_applicable(QName, Policy) ->
    case lookup(QName) of
        {ok, Q} ->
            rabbit_queue_type:is_policy_applicable(Q, Policy);
        _ ->
            %% Defaults to previous behaviour. Apply always
            true
    end.

is_server_named_allowed(Args) ->
    Type = get_queue_type(Args),
    rabbit_queue_type:is_server_named_allowed(Type).

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

-spec lookup_many ([name()]) -> [amqqueue:amqqueue()].

lookup_many(Names) when is_list(Names) ->
    lookup(Names).

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

-spec get_rebalance_lock(pid()) ->
    {true, {rebalance_queues, pid()}} | false.
get_rebalance_lock(Pid) when is_pid(Pid) ->
    Id = {rebalance_queues, Pid},
    Nodes = [node()|nodes()],
    %% Note that we're not re-trying. We want to immediately know
    %% if a re-balance is taking place and stop accordingly.
    case global:set_lock(Id, Nodes, 0) of
        true ->
            {true, Id};
        false ->
            false
    end.

-spec rebalance('all' | 'quorum' | 'classic', binary(), binary()) ->
                       {ok, [{node(), pos_integer()}]} | {error, term()}.
rebalance(Type, VhostSpec, QueueSpec) ->
    %% We have not yet acquired the rebalance_queues global lock.
    maybe_rebalance(get_rebalance_lock(self()), Type, VhostSpec, QueueSpec).

maybe_rebalance({true, Id}, Type, VhostSpec, QueueSpec) ->
    rabbit_log:info("Starting queue rebalance operation: '~s' for vhosts matching '~s' and queues matching '~s'",
                    [Type, VhostSpec, QueueSpec]),
    Running = rabbit_maintenance:filter_out_drained_nodes_consistent_read(rabbit_nodes:all_running()),
    NumRunning = length(Running),
    ToRebalance = [Q || Q <- rabbit_amqqueue:list(),
                        filter_per_type(Type, Q),
                        is_replicated(Q),
                        is_match(amqqueue:get_vhost(Q), VhostSpec) andalso
                            is_match(get_resource_name(amqqueue:get_name(Q)), QueueSpec)],
    NumToRebalance = length(ToRebalance),
    ByNode = group_by_node(ToRebalance),
    Rem = case (NumToRebalance rem NumRunning) of
            0 -> 0;
            _ -> 1
        end,
    MaxQueuesDesired = (NumToRebalance div NumRunning) + Rem,
    Result = iterative_rebalance(ByNode, MaxQueuesDesired),
    global:del_lock(Id),
    rabbit_log:info("Finished queue rebalance operation"),
    Result;
maybe_rebalance(false, _Type, _VhostSpec, _QueueSpec) ->
    rabbit_log:warning("Queue rebalance operation is in progress, please wait."),
    {error, rebalance_in_progress}.

%% Stream queues don't yet support rebalance
filter_per_type(all, Q)  ->
    ?amqqueue_is_quorum(Q) or ?amqqueue_is_classic(Q);
filter_per_type(quorum, Q) ->
    ?amqqueue_is_quorum(Q);
filter_per_type(classic, Q) ->
    ?amqqueue_is_classic(Q).

rebalance_module(Q) when ?amqqueue_is_quorum(Q) ->
    rabbit_quorum_queue;
rebalance_module(Q) when ?amqqueue_is_classic(Q) ->
    rabbit_mirror_queue_misc.

get_resource_name(#resource{name = Name}) ->
    Name.

get_resource_vhost_name(#resource{virtual_host = VHostName}) ->
    VHostName.

is_match(Subj, RegEx) ->
   nomatch /= re:run(Subj, RegEx).

iterative_rebalance(ByNode, MaxQueuesDesired) ->
    case maybe_migrate(ByNode, MaxQueuesDesired) of
        {ok, Summary} ->
            rabbit_log:info("All queue masters are balanced"),
            {ok, Summary};
        {migrated, Other} ->
            iterative_rebalance(Other, MaxQueuesDesired);
        {not_migrated, Other} ->
            iterative_rebalance(Other, MaxQueuesDesired)
    end.

maybe_migrate(ByNode, MaxQueuesDesired) ->
    maybe_migrate(ByNode, MaxQueuesDesired, maps:keys(ByNode)).

maybe_migrate(ByNode, _, []) ->
    {ok, maps:fold(fun(K, V, Acc) ->
                           {CQs, QQs} = lists:partition(fun({_, Q, _}) ->
                                                                ?amqqueue_is_classic(Q)
                                                        end, V),
                           [[{<<"Node name">>, K}, {<<"Number of quorum queues">>, length(QQs)},
                             {<<"Number of replicated classic queues">>, length(CQs)}] | Acc]
                   end, [], ByNode)};
maybe_migrate(ByNode, MaxQueuesDesired, [N | Nodes]) ->
    case maps:get(N, ByNode, []) of
        [{_, Q, false} = Queue | Queues] = All when length(All) > MaxQueuesDesired ->
            Name = amqqueue:get_name(Q),
            Module = rebalance_module(Q),
            Candidates = rabbit_maintenance:filter_out_drained_nodes_local_read(Module:get_replicas(Q) -- [N]),
            case Candidates of
                [] ->
                    {not_migrated, update_not_migrated_queue(N, Queue, Queues, ByNode)};
                _ ->
                    [{Length, Destination} | _] = sort_by_number_of_queues(Candidates, ByNode),
                    rabbit_log:warning("Migrating queue ~p from node ~p with ~p queues to node ~p with ~p queues",
                                       [Name, N, length(All), Destination, Length]),
                    case Module:transfer_leadership(Q, Destination) of
                        {migrated, NewNode} ->
                            rabbit_log:warning("Queue ~p migrated to ~p", [Name, NewNode]),
                            {migrated, update_migrated_queue(Destination, N, Queue, Queues, ByNode)};
                        {not_migrated, Reason} ->
                            rabbit_log:warning("Error migrating queue ~p: ~p", [Name, Reason]),
                            {not_migrated, update_not_migrated_queue(N, Queue, Queues, ByNode)}
                    end
            end;
        [{_, _, true} | _] = All when length(All) > MaxQueuesDesired ->
            rabbit_log:warning("Node ~p contains ~p queues, but all have already migrated. "
                               "Do nothing", [N, length(All)]),
            maybe_migrate(ByNode, MaxQueuesDesired, Nodes);
        All ->
            rabbit_log:warning("Node ~p only contains ~p queues, do nothing",
                               [N, length(All)]),
            maybe_migrate(ByNode, MaxQueuesDesired, Nodes)
    end.

update_not_migrated_queue(N, {Entries, Q, _}, Queues, ByNode) ->
    maps:update(N, Queues ++ [{Entries, Q, true}], ByNode).

update_migrated_queue(NewNode, OldNode, {Entries, Q, _}, Queues, ByNode) ->
    maps:update_with(NewNode,
                     fun(L) -> L ++ [{Entries, Q, true}] end,
                     [{Entries, Q, true}], maps:update(OldNode, Queues, ByNode)).

sort_by_number_of_queues(Nodes, ByNode) ->
    lists:keysort(1,
                  lists:map(fun(Node) ->
                                    {num_queues(Node, ByNode), Node}
                            end, Nodes)).

num_queues(Node, ByNode) ->
    length(maps:get(Node, ByNode, [])).

group_by_node(Queues) ->
    ByNode = lists:foldl(fun(Q, Acc) ->
                                 Module = rebalance_module(Q),
                                 Length = Module:queue_length(Q),
                                 maps:update_with(amqqueue:qnode(Q),
                                                  fun(L) -> [{Length, Q, false} | L] end,
                                                  [{Length, Q, false}], Acc)
                         end, #{}, Queues),
    maps:map(fun(_K, V) -> lists:keysort(1, V) end, ByNode).

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
        %% In that case a synchronised mirror can take over
        %% so we should retry.
        {ok, Q} when ?amqqueue_state_is(Q, stopped) ->
            %% The queue process was stopped by the supervisor
            rabbit_misc:with_exit_handler(
              fun () -> retry_wait(Q, F, E, RetriesLeft) end,
              fun () -> F(Q) end);
        %% The queue is supposed to be active.
        %% The master node can go away or queue can be killed
        %% so we retry, waiting for a mirror to take over.
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
        %% there are no mirrors to migrate to
        {stopped, false} ->
            E({absent, Q, stopped});
        _ ->
            case rabbit_mnesia:is_process_alive(QPid) of
                true ->
                    % rabbitmq-server#1682
                    % The old check would have crashed here,
                    % instead, log it and run the exit fun. absent & alive is weird,
                    % but better than crashing with badmatch,true
                    rabbit_log:debug("Unexpected alive queue process ~p", [QPid]),
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
      "failed to perform operation on ~s due to timeout", [rabbit_misc:rs(QueueName)]);

priv_absent(QueueName, QPid, _IsDurable, alive) ->
    rabbit_misc:protocol_error(
      not_found,
      "failed to perform operation on ~s: its master replica ~w may be stopping or being demoted",
      [rabbit_misc:rs(QueueName), QPid]).

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
     {<<"x-max-in-memory-length">>,    fun check_non_neg_int_arg/2},
     {<<"x-max-in-memory-bytes">>,     fun check_non_neg_int_arg/2},
     {<<"x-max-priority">>,            fun check_max_priority_arg/2},
     {<<"x-overflow">>,                fun check_overflow/2},
     {<<"x-queue-mode">>,              fun check_queue_mode/2},
     {<<"x-single-active-consumer">>,  fun check_single_active_consumer_arg/2},
     {<<"x-queue-type">>,              fun check_queue_type/2},
     {<<"x-quorum-initial-group-size">>,     fun check_initial_cluster_size_arg/2},
     {<<"x-max-age">>,                 fun check_max_age_arg/2},
     {<<"x-max-segment-size">>,        fun check_non_neg_int_arg/2},
     {<<"x-initial-cluster-size">>,    fun check_initial_cluster_size_arg/2},
     {<<"x-queue-leader-locator">>,    fun check_queue_leader_locator_arg/2}].

consume_args() -> [{<<"x-priority">>,              fun check_int_arg/2},
                   {<<"x-cancel-on-ha-failover">>, fun check_bool_arg/2},
                   {<<"x-stream-offset">>, fun check_stream_offset_arg/2}].

check_int_arg({Type, _}, _) ->
    case lists:member(Type, ?INTEGER_ARG_TYPES) of
        true  -> ok;
        false -> {error, rabbit_misc:format("expected integer, got ~p", [Type])}
    end;
check_int_arg(Val, _) when is_integer(Val) ->
    ok;
check_int_arg(_Val, _) ->
    {error, {unacceptable_type, "expected integer"}}.

check_bool_arg({bool, _}, _) -> ok;
check_bool_arg({Type, _}, _) -> {error, {unacceptable_type, Type}};
check_bool_arg(true, _)  -> ok;
check_bool_arg(false, _) -> ok;
check_bool_arg(_Val, _) -> {error, {unacceptable_type, "expected boolean"}}.

check_non_neg_int_arg({Type, Val}, Args) ->
    case check_int_arg({Type, Val}, Args) of
        ok when Val >= 0 -> ok;
        ok               -> {error, {value_negative, Val}};
        Error            -> Error
    end;
check_non_neg_int_arg(Val, Args) ->
    case check_int_arg(Val, Args) of
        ok when Val >= 0 -> ok;
        ok               -> {error, {value_negative, Val}};
        Error            -> Error
    end.

check_expires_arg({Type, Val}, Args) ->
    case check_int_arg({Type, Val}, Args) of
        ok when Val == 0 -> {error, {value_zero, Val}};
        ok               -> rabbit_misc:check_expiry(Val);
        Error            -> Error
    end;
check_expires_arg(Val, Args) ->
    case check_int_arg(Val, Args) of
        ok when Val == 0 -> {error, {value_zero, Val}};
        ok               -> rabbit_misc:check_expiry(Val);
        Error            -> Error
    end.

check_message_ttl_arg({Type, Val}, Args) ->
    case check_int_arg({Type, Val}, Args) of
        ok    -> rabbit_misc:check_expiry(Val);
        Error -> Error
    end;
check_message_ttl_arg(Val, Args) ->
    case check_int_arg(Val, Args) of
        ok    -> rabbit_misc:check_expiry(Val);
        Error -> Error
    end.

check_max_priority_arg({Type, Val}, Args) ->
    case check_non_neg_int_arg({Type, Val}, Args) of
        ok when Val =< ?MAX_SUPPORTED_PRIORITY -> ok;
        ok                                     -> {error, {max_value_exceeded, Val}};
        Error                                  -> Error
    end;
check_max_priority_arg(Val, Args) ->
    case check_non_neg_int_arg(Val, Args) of
        ok when Val =< ?MAX_SUPPORTED_PRIORITY -> ok;
        ok                                     -> {error, {max_value_exceeded, Val}};
        Error                                  -> Error
    end.

check_single_active_consumer_arg({Type, Val}, Args) ->
    check_bool_arg({Type, Val}, Args);
check_single_active_consumer_arg(Val, Args) ->
    check_bool_arg(Val, Args).

check_initial_cluster_size_arg({Type, Val}, Args) ->
    case check_non_neg_int_arg({Type, Val}, Args) of
        ok when Val == 0 -> {error, {value_zero, Val}};
        ok               -> ok;
        Error            -> Error
    end;
check_initial_cluster_size_arg(Val, Args) ->
    case check_non_neg_int_arg(Val, Args) of
        ok when Val == 0 -> {error, {value_zero, Val}};
        ok               -> ok;
        Error            -> Error
    end.

check_max_age_arg({longstr, Val}, _Args) ->
    case check_max_age(Val) of
        {error, _} = E ->
            E;
        _ ->
            ok
    end;
check_max_age_arg({Type,    _}, _Args) ->
    {error, {unacceptable_type, Type}}.

check_max_age(MaxAge) ->
    case re:run(MaxAge, "(^[0-9]*)(.*)", [{capture, all_but_first, list}]) of
        {match, [Value, Unit]} ->
            case list_to_integer(Value) of
                I when I > 0 ->
                    case lists:member(Unit, ["Y", "M", "D", "h", "m", "s"]) of
                        true ->
                            Int = list_to_integer(Value),
                            Int * unit_value_in_ms(Unit);
                        false ->
                            {error, invalid_max_age}
                    end;
                _ ->
                    {error, invalid_max_age}
            end;
        _ ->
            {error, invalid_max_age}
    end.

unit_value_in_ms("Y") ->
    365 * unit_value_in_ms("D");
unit_value_in_ms("M") ->
    30 * unit_value_in_ms("D");
unit_value_in_ms("D") ->
    24 * unit_value_in_ms("h");
unit_value_in_ms("h") ->
    3600 * unit_value_in_ms("s");
unit_value_in_ms("m") ->
    60 * unit_value_in_ms("s");
unit_value_in_ms("s") ->
    1000.

%% Note that the validity of x-dead-letter-exchange is already verified
%% by rabbit_channel's queue.declare handler.
check_dlxname_arg({longstr, _}, _) -> ok;
check_dlxname_arg({Type,    _}, _) -> {error, {unacceptable_type, Type}};
check_dlxname_arg(Val, _) when is_list(Val) or is_binary(Val) -> ok;
check_dlxname_arg(_Val, _) -> {error, {unacceptable_type, "expected a string (valid exchange name)"}}.

check_dlxrk_arg({longstr, _}, Args) ->
    case rabbit_misc:table_lookup(Args, <<"x-dead-letter-exchange">>) of
        undefined -> {error, routing_key_but_no_dlx_defined};
        _         -> ok
    end;
check_dlxrk_arg({Type,    _}, _Args) ->
    {error, {unacceptable_type, Type}};
check_dlxrk_arg(Val, Args) when is_binary(Val) ->
    case rabbit_misc:table_lookup(Args, <<"x-dead-letter-exchange">>) of
        undefined -> {error, routing_key_but_no_dlx_defined};
        _         -> ok
    end;
check_dlxrk_arg(_Val, _Args) ->
    {error, {unacceptable_type, "expected a string"}}.

-define(KNOWN_OVERFLOW_MODES, [<<"drop-head">>, <<"reject-publish">>, <<"reject-publish-dlx">>]).
check_overflow({longstr, Val}, _Args) ->
    case lists:member(Val, ?KNOWN_OVERFLOW_MODES) of
        true  -> ok;
        false -> {error, invalid_overflow}
    end;
check_overflow({Type,    _}, _Args) ->
    {error, {unacceptable_type, Type}};
check_overflow(Val, _Args) when is_binary(Val) ->
    case lists:member(Val, ?KNOWN_OVERFLOW_MODES) of
        true  -> ok;
        false -> {error, invalid_overflow}
    end;
check_overflow(_Val, _Args) ->
    {error, invalid_overflow}.

-define(KNOWN_LEADER_LOCATORS, [<<"client-local">>, <<"random">>, <<"least-leaders">>]).
check_queue_leader_locator_arg({longstr, Val}, _Args) ->
    case lists:member(Val, ?KNOWN_LEADER_LOCATORS) of
        true  -> ok;
        false -> {error, invalid_queue_locator_arg}
    end;
check_queue_leader_locator_arg({Type, _}, _Args) ->
    {error, {unacceptable_type, Type}};
check_queue_leader_locator_arg(Val, _Args) when is_binary(Val) ->
    case lists:member(Val, ?KNOWN_LEADER_LOCATORS) of
        true  -> ok;
        false -> {error, invalid_queue_locator_arg}
    end;
check_queue_leader_locator_arg(_Val, _Args) ->
    {error, invalid_queue_locator_arg}.

check_stream_offset_arg(Val, _Args) ->
    case rabbit_stream_queue:parse_offset_arg(Val) of
        {ok, _} ->
            ok;
        {error, _} ->
            {error, {invalid_stream_offset_arg, Val}}
    end.

-define(KNOWN_QUEUE_MODES, [<<"default">>, <<"lazy">>]).
check_queue_mode({longstr, Val}, _Args) ->
    case lists:member(Val, ?KNOWN_QUEUE_MODES) of
        true  -> ok;
        false -> {error, rabbit_misc:format("unsupported queue mode '~s'", [Val])}
    end;
check_queue_mode({Type,    _}, _Args) ->
    {error, {unacceptable_type, Type}};
check_queue_mode(Val, _Args) when is_binary(Val) ->
    case lists:member(Val, ?KNOWN_QUEUE_MODES) of
        true  -> ok;
        false -> {error, rabbit_misc:format("unsupported queue mode '~s'", [Val])}
    end;
check_queue_mode(_Val, _Args) ->
    {error, invalid_queue_mode}.

-define(KNOWN_QUEUE_TYPES, [<<"classic">>, <<"quorum">>, <<"stream">>]).
check_queue_type({longstr, Val}, _Args) ->
    case lists:member(Val, ?KNOWN_QUEUE_TYPES) of
        true  -> ok;
        false -> {error, rabbit_misc:format("unsupported queue type '~s'", [Val])}
    end;
check_queue_type({Type,    _}, _Args) ->
    {error, {unacceptable_type, Type}};
check_queue_type(Val, _Args) when is_binary(Val) ->
    case lists:member(Val, ?KNOWN_QUEUE_TYPES) of
        true  -> ok;
        false -> {error, rabbit_misc:format("unsupported queue type '~s'", [Val])}
    end;
check_queue_type(_Val, _Args) ->
    {error, invalid_queue_type}.

-spec list() -> [amqqueue:amqqueue()].

list() ->
    list_with_possible_retry(fun do_list/0).

do_list() ->
    mnesia:dirty_match_object(rabbit_queue, amqqueue:pattern_match_all()).

-spec count() -> non_neg_integer().

count() ->
    mnesia:table_info(rabbit_queue, size).

-spec list_names() -> [rabbit_amqqueue:name()].

list_names() -> mnesia:dirty_all_keys(rabbit_queue).

list_names(VHost) -> [amqqueue:get_name(Q) || Q <- list(VHost)].

list_local_names() ->
    [ amqqueue:get_name(Q) || Q <- list(),
           amqqueue:get_state(Q) =/= crashed, is_local_to_node(amqqueue:get_pid(Q), node())].

list_local_names_down() ->
    [ amqqueue:get_name(Q) || Q <- list(),
                              is_local_to_node(amqqueue:get_pid(Q), node()),
                              is_down(Q)].

is_down(Q) ->
    try
        info(Q, [state]) == [{state, down}]
    catch
        _:_ ->
            true
    end.


-spec sample_local_queues() -> [amqqueue:amqqueue()].
sample_local_queues() -> sample_n_by_name(list_local_names(), 300).

-spec sample_n_by_name([rabbit_amqqueue:name()], pos_integer()) -> [amqqueue:amqqueue()].
sample_n_by_name([], _N) ->
    [];
sample_n_by_name(Names, N) when is_list(Names) andalso is_integer(N) andalso N > 0 ->
    %% lists:nth/2 throws when position is > list length
    M = erlang:min(N, length(Names)),
    Ids = lists:foldl(fun( _, Acc) when length(Acc) >= 100 ->
                            Acc;
                        (_, Acc) ->
                            Pick = lists:nth(rand:uniform(M), Names),
                            [Pick | Acc]
                     end,
         [], lists:seq(1, M)),
    lists:map(fun (Id) ->
                {ok, Q} = rabbit_amqqueue:lookup(Id),
                Q
              end,
              lists:usort(Ids)).

-spec sample_n([amqqueue:amqqueue()], pos_integer()) -> [amqqueue:amqqueue()].
sample_n([], _N) ->
    [];
sample_n(Queues, N) when is_list(Queues) andalso is_integer(N) andalso N > 0 ->
    Names = [amqqueue:get_name(Q) || Q <- Queues],
    sample_n_by_name(Names, N).


-spec list_by_type(atom()) -> [amqqueue:amqqueue()].

list_by_type(classic) -> list_by_type(rabbit_classic_queue);
list_by_type(quorum)  -> list_by_type(rabbit_quorum_queue);
list_by_type(stream)  -> list_by_type(rabbit_stream_queue);
list_by_type(Type) ->
    {atomic, Qs} =
        mnesia:sync_transaction(
          fun () ->
                  mnesia:match_object(rabbit_durable_queue,
                                      amqqueue:pattern_match_on_type(Type),
                                      read)
          end),
    Qs.

-spec list_local_quorum_queue_names() -> [rabbit_amqqueue:name()].

list_local_quorum_queue_names() ->
    [ amqqueue:get_name(Q) || Q <- list_by_type(quorum),
           amqqueue:get_state(Q) =/= crashed,
      lists:member(node(), get_quorum_nodes(Q))].

-spec list_local_quorum_queues() -> [amqqueue:amqqueue()].
list_local_quorum_queues() ->
    [ Q || Q <- list_by_type(quorum),
      amqqueue:get_state(Q) =/= crashed,
      lists:member(node(), get_quorum_nodes(Q))].

-spec list_local_stream_queues() -> [amqqueue:amqqueue()].
list_local_stream_queues() ->
    [ Q || Q <- list_by_type(stream),
      amqqueue:get_state(Q) =/= crashed,
      lists:member(node(), get_quorum_nodes(Q))].

-spec list_local_leaders() -> [amqqueue:amqqueue()].
list_local_leaders() ->
    [ Q || Q <- list(),
         amqqueue:is_quorum(Q),
         amqqueue:get_state(Q) =/= crashed, amqqueue:get_leader(Q) =:= node()].

-spec list_local_followers() -> [amqqueue:amqqueue()].
list_local_followers() ->
    [Q
      || Q <- list(),
         amqqueue:is_quorum(Q),
         amqqueue:get_state(Q) =/= crashed,
         amqqueue:get_leader(Q) =/= node(),
         rabbit_quorum_queue:is_recoverable(Q)
         ].

-spec list_local_mirrored_classic_queues() -> [amqqueue:amqqueue()].
list_local_mirrored_classic_queues() ->
    [ Q || Q <- list(),
        amqqueue:get_state(Q) =/= crashed,
        amqqueue:is_classic(Q),
        is_local_to_node(amqqueue:get_pid(Q), node()),
        is_replicated(Q)].

-spec list_local_mirrored_classic_names() -> [rabbit_amqqueue:name()].
list_local_mirrored_classic_names() ->
    [ amqqueue:get_name(Q) || Q <- list(),
           amqqueue:get_state(Q) =/= crashed,
           amqqueue:is_classic(Q),
           is_local_to_node(amqqueue:get_pid(Q), node()),
           is_replicated(Q)].

-spec list_local_mirrored_classic_without_synchronised_mirrors() ->
    [amqqueue:amqqueue()].
list_local_mirrored_classic_without_synchronised_mirrors() ->
    [ Q || Q <- list(),
         amqqueue:get_state(Q) =/= crashed,
         amqqueue:is_classic(Q),
         %% filter out exclusive queues as they won't actually be mirrored
         is_not_exclusive(Q),
         is_local_to_node(amqqueue:get_pid(Q), node()),
         is_replicated(Q),
         not has_synchronised_mirrors_online(Q)].

-spec list_local_mirrored_classic_without_synchronised_mirrors_for_cli() ->
    [#{binary => any()}].
list_local_mirrored_classic_without_synchronised_mirrors_for_cli() ->
    ClassicQs = list_local_mirrored_classic_without_synchronised_mirrors(),
    [begin
         #resource{name = Name} = amqqueue:get_name(Q),
         #{
             <<"readable_name">> => rabbit_data_coercion:to_binary(rabbit_misc:rs(amqqueue:get_name(Q))),
             <<"name">>          => Name,
             <<"virtual_host">>  => amqqueue:get_vhost(Q),
             <<"type">>          => <<"classic">>
         }
     end || Q <- ClassicQs].

-spec list_local_quorum_queues_with_name_matching(binary()) -> [amqqueue:amqqueue()].
list_local_quorum_queues_with_name_matching(Pattern) ->
    [ Q || Q <- list_by_type(quorum),
      amqqueue:get_state(Q) =/= crashed,
      lists:member(node(), get_quorum_nodes(Q)),
      is_match(get_resource_name(amqqueue:get_name(Q)), Pattern)].

-spec list_local_quorum_queues_with_name_matching(vhost:name(), binary()) -> [amqqueue:amqqueue()].
list_local_quorum_queues_with_name_matching(VHostName, Pattern) ->
    [ Q || Q <- list_by_type(quorum),
      amqqueue:get_state(Q) =/= crashed,
      lists:member(node(), get_quorum_nodes(Q)),
      is_in_virtual_host(Q, VHostName),
      is_match(get_resource_name(amqqueue:get_name(Q)), Pattern)].

is_local_to_node(QPid, Node) when ?IS_CLASSIC(QPid) ->
    Node =:= node(QPid);
is_local_to_node({_, Leader} = QPid, Node) when ?IS_QUORUM(QPid) ->
    Node =:= Leader;
is_local_to_node(_QPid, _Node) ->
    false.

is_in_virtual_host(Q, VHostName) ->
    VHostName =:= get_resource_vhost_name(amqqueue:get_name(Q)).

-spec list(vhost:name()) -> [amqqueue:amqqueue()].
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
    rabbit_log:error("Failed to fetch number of queues in vhost ~p:~n~p",
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

%% It should no default to classic queue keys, but a subset of those that must be shared
%% by all queue types. Not sure this is even being used, so will leave it here for backwards
%% compatibility. Each queue type handles now info(Q, all_keys) with the keys it supports.
info_keys() -> rabbit_amqqueue_process:info_keys().

map(Qs, F) -> rabbit_misc:filter_exit_map(F, Qs).

is_unresponsive(Q, _Timeout) when ?amqqueue_state_is(Q, crashed) ->
    false;
is_unresponsive(Q, Timeout) when ?amqqueue_is_classic(Q) ->
    QPid = amqqueue:get_pid(Q),
    try
        delegate:invoke(QPid, {gen_server2, call, [{info, [name]}, Timeout]}),
        false
    catch
        %% TODO catch any exit??
        exit:{timeout, _} ->
            true
    end;
is_unresponsive(Q, Timeout) when ?amqqueue_is_quorum(Q) ->
    try
        Leader = amqqueue:get_pid(Q),
        case rabbit_fifo_client:stat(Leader, Timeout) of
          {ok, _, _}   -> false;
          {timeout, _} -> true;
          {error, _}   -> true
        end
    catch
        exit:{timeout, _} ->
            true
    end;
is_unresponsive(Q, Timeout) when ?amqqueue_is_stream(Q) ->
    try
        #{leader_pid := LeaderPid} = amqqueue:get_type_state(Q),
        case gen_batch_server:call(LeaderPid, get_reader_context, Timeout) of
            #{dir := _} -> false;
            _ -> true
        end
    catch
        exit:{timeout, _} ->
            true
    end.

format(Q) when ?amqqueue_is_quorum(Q) -> rabbit_quorum_queue:format(Q);
format(Q) -> rabbit_amqqueue_process:format(Q).

-spec info(amqqueue:amqqueue()) -> rabbit_types:infos().

info(Q) when ?is_amqqueue(Q) -> rabbit_queue_type:info(Q, all_keys).


-spec info(amqqueue:amqqueue(), rabbit_types:info_keys()) ->
          rabbit_types:infos().

info(Q, Items) when ?is_amqqueue(Q) ->
    rabbit_queue_type:info(Q, Items).

info_down(Q, DownReason) ->
    rabbit_queue_type:info_down(Q, DownReason).

info_down(Q, Items, DownReason) ->
    rabbit_queue_type:info_down(Q, Items, DownReason).

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

collect_info_all(VHostPath, Items) ->
    Nodes = rabbit_nodes:all_running(),
    Ref = make_ref(),
    Pids = [ spawn_link(Node, rabbit_amqqueue, emit_info_local, [VHostPath, Items, Ref, self()]) || Node <- Nodes ],
    rabbit_control_misc:await_emitters_termination(Pids),
    wait_for_queues(Ref, length(Pids), []).

wait_for_queues(Ref, N, Acc) ->
    receive
        {Ref, finished} when N == 1 ->
            Acc;
        {Ref, finished} ->
            wait_for_queues(Ref, N - 1, Acc);
        {Ref, Items, continue} ->
            wait_for_queues(Ref, N, [Items | Acc])
    after
        1000 ->
            Acc
    end.

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

% Note: https://www.pivotaltracker.com/story/show/166962656
% This event is necessary for the stats timer to be initialized with
% the correct values once the management agent has started
force_event_refresh(Ref) ->
    %% note: quorum queuse emit stats on periodic ticks that run unconditionally,
    %%       so force_event_refresh is unnecessary (and, in fact, would only produce log noise) for QQs.
    ClassicQs = list_by_type(rabbit_classic_queue),
    [gen_server2:cast(amqqueue:get_pid(Q),
                      {force_event_refresh, Ref}) || Q <- ClassicQs],
    ok.

-spec notify_policy_changed(amqqueue:amqqueue()) -> 'ok'.
notify_policy_changed(Q) when ?is_amqqueue(Q) ->
    rabbit_queue_type:policy_changed(Q).

-spec consumers(amqqueue:amqqueue()) ->
          [{pid(), rabbit_types:ctag(), boolean(), non_neg_integer(),
            boolean(), atom(),
            rabbit_framing:amqp_table(), rabbit_types:username()}].

consumers(Q) when ?amqqueue_is_classic(Q) ->
    QPid = amqqueue:get_pid(Q),
    delegate:invoke(QPid, {gen_server2, call, [consumers, infinity]});
consumers(Q) when ?amqqueue_is_quorum(Q) ->
    QPid = amqqueue:get_pid(Q),
    case ra:local_query(QPid, fun rabbit_fifo:query_consumers/1) of
        {ok, {_, Result}, _} -> maps:values(Result);
        _                    -> []
    end;
consumers(Q) when ?amqqueue_is_stream(Q) ->
    %% TODO how??? they only exist on the channel
    %% we could list the offset listener on the writer but we don't even have a consumer tag,
    %% only a (channel) pid and offset
    [].

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
stat(Q) ->
    rabbit_queue_type:stat(Q).

-spec pid_of(amqqueue:amqqueue()) ->
          pid().

pid_of(Q) -> amqqueue:get_pid(Q).

-spec pid_of(rabbit_types:vhost(), rabbit_misc:resource_name()) ->
          pid() | rabbit_types:error('not_found').

pid_of(VHost, QueueName) ->
  case lookup(rabbit_misc:r(VHost, queue, QueueName)) of
    {ok, Q}                -> pid_of(Q);
    {error, not_found} = E -> E
  end.

-spec delete_exclusive(qpids(), pid()) -> 'ok'.

delete_exclusive(QPids, ConnId) ->
    rabbit_amqqueue_common:delete_exclusive(QPids, ConnId).

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
            qlen() |
            {protocol_error, Type :: atom(), Reason :: string(), Args :: term()};
        (amqqueue:amqqueue(), 'true' , 'false', rabbit_types:username()) ->
            qlen() | rabbit_types:error('in_use') |
            {protocol_error, Type :: atom(), Reason :: string(), Args :: term()};
        (amqqueue:amqqueue(), 'false', 'true', rabbit_types:username()) ->
            qlen() | rabbit_types:error('not_empty') |
            {protocol_error, Type :: atom(), Reason :: string(), Args :: term()};
        (amqqueue:amqqueue(), 'true' , 'true', rabbit_types:username()) ->
            qlen() |
            rabbit_types:error('in_use') |
            rabbit_types:error('not_empty') |
            {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.
delete(Q, IfUnused, IfEmpty, ActingUser) ->
    rabbit_queue_type:delete(Q, IfUnused, IfEmpty, ActingUser).

%% delete_crashed* INCLUDED FOR BACKWARDS COMPATBILITY REASONS
delete_crashed(Q) when ?amqqueue_is_classic(Q) ->
    rabbit_classic_queue:delete_crashed(Q).

delete_crashed(Q, ActingUser) when ?amqqueue_is_classic(Q) ->
    rabbit_classic_queue:delete_crashed(Q, ActingUser).

-spec delete_crashed_internal(amqqueue:amqqueue(), rabbit_types:username()) -> 'ok'.
delete_crashed_internal(Q, ActingUser) when ?amqqueue_is_classic(Q) ->
    rabbit_classic_queue:delete_crashed_internal(Q, ActingUser).

-spec purge(amqqueue:amqqueue()) -> qlen().
purge(Q) when ?is_amqqueue(Q) ->
    rabbit_queue_type:purge(Q).

-spec requeue(name(),
              {rabbit_fifo:consumer_tag(), [msg_id()]},
              rabbit_queue_type:state()) ->
    {ok, rabbit_queue_type:state(), rabbit_queue_type:actions()}.
requeue(QRef, {CTag, MsgIds}, QStates) ->
    reject(QRef, true, {CTag, MsgIds}, QStates).

-spec ack(name(),
          {rabbit_fifo:consumer_tag(), [msg_id()]},
          rabbit_queue_type:state()) ->
    {ok, rabbit_queue_type:state(), rabbit_queue_type:actions()}.
ack(QPid, {CTag, MsgIds}, QueueStates) ->
    rabbit_queue_type:settle(QPid, complete, CTag, MsgIds, QueueStates).


-spec reject(name(),
             boolean(),
             {rabbit_fifo:consumer_tag(), [msg_id()]},
             rabbit_queue_type:state()) ->
    {ok, rabbit_queue_type:state(), rabbit_queue_type:actions()}.
reject(QRef, Requeue, {CTag, MsgIds}, QStates) ->
    Op = case Requeue of
             true -> requeue;
             false -> discard
         end,
    rabbit_queue_type:settle(QRef, Op, CTag, MsgIds, QStates).

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
             rabbit_types:ctag(),
             non_neg_integer(),
             boolean(),
             rabbit_queue_type:state()) ->
    {ok, rabbit_queue_type:state(), rabbit_queue_type:actions()}.
credit(Q, CTag, Credit, Drain, QStates) ->
    rabbit_queue_type:credit(Q, CTag, Credit, Drain, QStates).

-spec basic_get(amqqueue:amqqueue(), boolean(), pid(), rabbit_types:ctag(),
                rabbit_queue_type:state()) ->
          {'ok', non_neg_integer(), qmsg(), rabbit_queue_type:state()} |
          {'empty', rabbit_queue_type:state()} |
          {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.
basic_get(Q, NoAck, LimiterPid, CTag, QStates0) ->
    rabbit_queue_type:dequeue(Q, NoAck, LimiterPid, CTag, QStates0).


-spec basic_consume(amqqueue:amqqueue(), boolean(), pid(), pid(), boolean(),
                    non_neg_integer(), rabbit_types:ctag(), boolean(),
                    rabbit_framing:amqp_table(), any(), rabbit_types:username(),
                    rabbit_queue_type:state()) ->
    {ok, rabbit_queue_type:state(), rabbit_queue_type:actions()} |
    {error, term()} |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.
basic_consume(Q, NoAck, ChPid, LimiterPid,
              LimiterActive, ConsumerPrefetchCount, ConsumerTag,
              ExclusiveConsume, Args, OkMsg, ActingUser, Contexts) ->

    QName = amqqueue:get_name(Q),
    %% first phase argument validation
    %% each queue type may do further validations
    ok = check_consume_arguments(QName, Args),
    Spec = #{no_ack => NoAck,
             channel_pid => ChPid,
             limiter_pid => LimiterPid,
             limiter_active => LimiterActive,
             prefetch_count => ConsumerPrefetchCount,
             consumer_tag => ConsumerTag,
             exclusive_consume => ExclusiveConsume,
             args => Args,
             ok_msg => OkMsg,
             acting_user =>  ActingUser},
    rabbit_queue_type:consume(Q, Spec, Contexts).

-spec basic_cancel(amqqueue:amqqueue(), rabbit_types:ctag(), any(),
                   rabbit_types:username(),
                   rabbit_queue_type:state()) ->
    {ok, rabbit_queue_type:state()} | {error, term()}.
basic_cancel(Q, ConsumerTag, OkMsg, ActingUser, QStates) ->
    rabbit_queue_type:cancel(Q, ConsumerTag,
                             OkMsg, ActingUser, QStates).

-spec notify_decorators(amqqueue:amqqueue()) -> 'ok'.

notify_decorators(Q) ->
    rabbit_queue_type:notify_decorators(Q).

notify_sent(QPid, ChPid) ->
    rabbit_amqqueue_common:notify_sent(QPid, ChPid).

notify_sent_queue_down(QPid) ->
    rabbit_amqqueue_common:notify_sent_queue_down(QPid).

-spec resume(pid(), pid()) -> 'ok'.

resume(QPid, ChPid) -> delegate:invoke_no_result(QPid, {gen_server2, cast,
                                                        [{resume, ChPid}]}).

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

%% Try to promote a mirror while down - it should recover as a
%% master. We try to take the oldest mirror here for best chance of
%% recovery.
forget_node_for_queue(_DeadNode, Q)
  when ?amqqueue_is_quorum(Q) ->
    ok;
forget_node_for_queue(DeadNode, Q) ->
    RS = amqqueue:get_recoverable_slaves(Q),
    forget_node_for_queue(DeadNode, RS, Q).

forget_node_for_queue(_DeadNode, [], Q) ->
    %% No mirrors to recover from, queue is gone.
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
        {true, rabbit_classic_queue} ->
            Q1 = amqqueue:set_pid(Q, rabbit_misc:node_to_fake_pid(H)),
            ok = mnesia:write(rabbit_durable_queue, Q1, write);
        {true, rabbit_quorum_queue} ->
            ok
    end.

node_permits_offline_promotion(Node) ->
    case node() of
        Node -> not rabbit:is_running(); %% [1]
        _    -> All = rabbit_mnesia:cluster_nodes(all),
                Running = rabbit_nodes:all_running(),
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

is_replicated(Q) when ?amqqueue_is_classic(Q) ->
    rabbit_mirror_queue_misc:is_mirrored(Q);
is_replicated(_Q) ->
    %% streams and quorum queues are all replicated
    true.

is_exclusive(Q) when ?amqqueue_exclusive_owner_is(Q, none) ->
    false;
is_exclusive(Q) when ?amqqueue_exclusive_owner_is_pid(Q) ->
    true.

is_not_exclusive(Q) ->
    not is_exclusive(Q).

is_dead_exclusive(Q) when ?amqqueue_exclusive_owner_is(Q, none) ->
    false;
is_dead_exclusive(Q) when ?amqqueue_exclusive_owner_is_pid(Q) ->
    Pid = amqqueue:get_pid(Q),
    not rabbit_mnesia:is_process_alive(Pid).

-spec has_synchronised_mirrors_online(amqqueue:amqqueue()) -> boolean().
has_synchronised_mirrors_online(Q) ->
    %% a queue with all mirrors down would have no mirror pids.
    %% We treat these as in sync intentionally to avoid false positives.
    MirrorPids = amqqueue:get_sync_slave_pids(Q),
    MirrorPids =/= [] andalso lists:any(fun rabbit_misc:is_process_alive/1, MirrorPids).

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
            %% by the incoming mirror node and this function, called
            %% by the master node. If this function is executed after
            %% record_synchronised/1, the node is erroneously removed
            %% from the recoverable mirrors list.
            %%
            %% We check if the mirror node's queue PID is alive. If it is
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
                 rabbit_classic_queue % Type
                ).

-spec immutable(amqqueue:amqqueue()) -> amqqueue:amqqueue().

immutable(Q) -> amqqueue:set_immutable(Q).

-spec deliver([amqqueue:amqqueue()], rabbit_types:delivery()) -> 'ok'.

deliver(Qs, Delivery) ->
    _ = rabbit_queue_type:deliver(Qs, Delivery, stateless),
    ok.

get_quorum_nodes(Q) ->
    case amqqueue:get_type_state(Q) of
        #{nodes := Nodes} ->
            Nodes;
        _ ->
            []
    end.
