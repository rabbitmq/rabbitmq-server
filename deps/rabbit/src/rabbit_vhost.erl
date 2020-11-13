%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_vhost).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-export([recover/0, recover/1]).
-export([add/2, delete/2, exists/1, list/0, count/0, with/2, with_user_and_vhost/3, assert/1, update/2,
         set_limits/2, limits_of/1, vhost_cluster_state/1, is_running_on_all_nodes/1, await_running_on_all_nodes/2]).
-export([info/1, info/2, info_all/0, info_all/1, info_all/2, info_all/3]).
-export([dir/1, msg_store_dir_path/1, msg_store_dir_wildcard/0]).
-export([delete_storage/1]).
-export([vhost_down/1]).

-spec add(rabbit_types:vhost(), rabbit_types:username()) -> rabbit_types:ok_or_error(any()).
-spec delete(rabbit_types:vhost(), rabbit_types:username()) -> rabbit_types:ok_or_error(any()).
-spec update(rabbit_types:vhost(), fun((#vhost{}) -> #vhost{})) -> #vhost{}.
-spec exists(rabbit_types:vhost()) -> boolean().
-spec list() -> [rabbit_types:vhost()].
-spec with_user_and_vhost
        (rabbit_types:username(), rabbit_types:vhost(), rabbit_misc:thunk(A)) -> A.
-spec assert(rabbit_types:vhost()) -> 'ok'.

-spec info(rabbit_types:vhost()) -> rabbit_types:infos().
-spec info(rabbit_types:vhost(), rabbit_types:info_keys())
                -> rabbit_types:infos().
-spec info_all() -> [rabbit_types:infos()].
-spec info_all(rabbit_types:info_keys()) -> [rabbit_types:infos()].
-spec info_all(rabbit_types:info_keys(), reference(), pid()) ->
                         'ok'.

recover() ->
    %% Clear out remnants of old incarnation, in case we restarted
    %% faster than other nodes handled DOWN messages from us.
    rabbit_amqqueue:on_node_down(node()),

    rabbit_amqqueue:warn_file_limit(),

    %% Prepare rabbit_semi_durable_route table
    rabbit_binding:recover(),

    %% rabbit_vhost_sup_sup will start the actual recovery.
    %% So recovery will be run every time a vhost supervisor is restarted.
    ok = rabbit_vhost_sup_sup:start(),

    [ok = rabbit_vhost_sup_sup:init_vhost(VHost) || VHost <- rabbit_vhost:list()],
    ok.

recover(VHost) ->
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    rabbit_log:info("Making sure data directory '~ts' for vhost '~s' exists~n",
                    [VHostDir, VHost]),
    VHostStubFile = filename:join(VHostDir, ".vhost"),
    ok = rabbit_file:ensure_dir(VHostStubFile),
    ok = file:write_file(VHostStubFile, VHost),
    {RecoveredQs, FailedQs} = rabbit_amqqueue:recover(VHost),
    AllQs = RecoveredQs ++ FailedQs,
    ok = rabbit_binding:recover(
            rabbit_exchange:recover(VHost),
            [QName || #amqqueue{name = QName} <- AllQs]),
    ok = rabbit_amqqueue:start(RecoveredQs),
    %% Start queue mirrors.
    ok = rabbit_mirror_queue_misc:on_vhost_up(VHost),
    ok.

%%----------------------------------------------------------------------------

-define(INFO_KEYS, [name, tracing, cluster_state]).

add(VHost, ActingUser) ->
    case exists(VHost) of
        true  -> ok;
        false -> do_add(VHost, ActingUser)
    end.

do_add(VHostPath, ActingUser) ->
    rabbit_log:info("Adding vhost '~s'~n", [VHostPath]),
    R = rabbit_misc:execute_mnesia_transaction(
          fun () ->
                  case mnesia:wread({rabbit_vhost, VHostPath}) of
                      []  -> ok = mnesia:write(rabbit_vhost,
                                               #vhost{virtual_host = VHostPath},
                                               write);
                      %% the vhost already exists
                      [_] -> ok
                  end
          end,
          fun (ok, true) ->
                  ok;
              (ok, false) ->
                  [_ = rabbit_exchange:declare(
                     rabbit_misc:r(VHostPath, exchange, Name),
                     Type, true, false, Internal, [], ActingUser) ||
                      {Name, Type, Internal} <-
                          [{<<"">>,                   direct,  false},
                           {<<"amq.direct">>,         direct,  false},
                           {<<"amq.topic">>,          topic,   false},
                           %% per 0-9-1 pdf
                           {<<"amq.match">>,          headers, false},
                           %% per 0-9-1 xml
                           {<<"amq.headers">>,        headers, false},
                           {<<"amq.fanout">>,         fanout,  false},
                           {<<"amq.rabbitmq.trace">>, topic,   true}]],
                  ok
          end),
    case rabbit_vhost_sup_sup:start_on_all_nodes(VHostPath) of
        ok ->
            rabbit_event:notify(vhost_created, info(VHostPath)
                                ++ [{user_who_performed_action, ActingUser}]),
            R;
        {error, Reason} ->
            Msg = rabbit_misc:format("failed to set up vhost '~s': ~p",
                                     [VHostPath, Reason]),
            {error, Msg}
    end.

delete(VHostPath, ActingUser) ->
    %% FIXME: We are forced to delete the queues and exchanges outside
    %% the TX below. Queue deletion involves sending messages to the queue
    %% process, which in turn results in further mnesia actions and
    %% eventually the termination of that process. Exchange deletion causes
    %% notifications which must be sent outside the TX
    rabbit_log:info("Deleting vhost '~s'~n", [VHostPath]),
    QDelFun = fun (Q) -> rabbit_amqqueue:delete(Q, false, false, ActingUser) end,
    [assert_benign(rabbit_amqqueue:with(Name, QDelFun), ActingUser) ||
        #amqqueue{name = Name} <- rabbit_amqqueue:list(VHostPath)],
    [assert_benign(rabbit_exchange:delete(Name, false, ActingUser), ActingUser) ||
        #exchange{name = Name} <- rabbit_exchange:list(VHostPath)],
    Funs = rabbit_misc:execute_mnesia_transaction(
          with(VHostPath, fun () -> internal_delete(VHostPath, ActingUser) end)),
    ok = rabbit_event:notify(vhost_deleted, [{name, VHostPath},
                                             {user_who_performed_action, ActingUser}]),
    [case Fun() of
         ok                                  -> ok;
         {error, {no_such_vhost, VHostPath}} -> ok
     end || Fun <- Funs],
    %% After vhost was deleted from mnesia DB, we try to stop vhost supervisors
    %% on all the nodes.
    rabbit_vhost_sup_sup:delete_on_all_nodes(VHostPath),
    ok.

%% 50 ms
-define(AWAIT_SAMPLE_INTERVAL, 50).

-spec await_running_on_all_nodes(rabbit_types:vhost(), integer()) -> ok | {error, timeout}.
await_running_on_all_nodes(VHost, Timeout) ->
    Attempts = round(Timeout / ?AWAIT_SAMPLE_INTERVAL),
    await_running_on_all_nodes0(VHost, Attempts).

await_running_on_all_nodes0(_VHost, 0) ->
    {error, timeout};
await_running_on_all_nodes0(VHost, Attempts) ->
    case is_running_on_all_nodes(VHost) of
        true  -> ok;
        _     ->
            timer:sleep(?AWAIT_SAMPLE_INTERVAL),
            await_running_on_all_nodes0(VHost, Attempts - 1)
    end.

-spec is_running_on_all_nodes(rabbit_types:vhost()) -> boolean().
is_running_on_all_nodes(VHost) ->
    States = vhost_cluster_state(VHost),
    lists:all(fun ({_Node, State}) -> State =:= running end,
              States).

-spec vhost_cluster_state(rabbit_types:vhost()) -> [{atom(), atom()}].
vhost_cluster_state(VHost) ->
    Nodes = rabbit_nodes:all_running(),
    lists:map(fun(Node) ->
        State = case rabbit_misc:rpc_call(Node,
                                          rabbit_vhost_sup_sup, is_vhost_alive,
                                          [VHost]) of
            {badrpc, nodedown} -> nodedown;
            true               -> running;
            false              -> stopped
        end,
        {Node, State}
    end,
    Nodes).

vhost_down(VHostPath) ->
    ok = rabbit_event:notify(vhost_down,
                             [{name, VHostPath},
                              {node, node()},
                              {user_who_performed_action, ?INTERNAL_USER}]).

delete_storage(VHost) ->
    VhostDir = msg_store_dir_path(VHost),
    rabbit_log:info("Deleting message store directory for vhost '~s' at '~s'~n", [VHost, VhostDir]),
    %% Message store should be closed when vhost supervisor is closed.
    ok = rabbit_file:recursive_delete([VhostDir]).

assert_benign(ok, _)                 -> ok;
assert_benign({ok, _}, _)            -> ok;
assert_benign({error, not_found}, _) -> ok;
assert_benign({error, {absent, Q, _}}, ActingUser) ->
    %% Removing the mnesia entries here is safe. If/when the down node
    %% restarts, it will clear out the on-disk storage of the queue.
    rabbit_amqqueue:internal_delete(Q#amqqueue.name, ActingUser).

internal_delete(VHostPath, ActingUser) ->
    [ok = rabbit_auth_backend_internal:clear_permissions(
            proplists:get_value(user, Info), VHostPath, ActingUser)
     || Info <- rabbit_auth_backend_internal:list_vhost_permissions(VHostPath)],
    TopicPermissions = rabbit_auth_backend_internal:list_vhost_topic_permissions(VHostPath),
    [ok = rabbit_auth_backend_internal:clear_topic_permissions(
        proplists:get_value(user, TopicPermission), VHostPath, ActingUser)
     || TopicPermission <- TopicPermissions],
    Fs1 = [rabbit_runtime_parameters:clear(VHostPath,
                                           proplists:get_value(component, Info),
                                           proplists:get_value(name, Info),
                                           ActingUser)
     || Info <- rabbit_runtime_parameters:list(VHostPath)],
    Fs2 = [rabbit_policy:delete(VHostPath, proplists:get_value(name, Info), ActingUser)
           || Info <- rabbit_policy:list(VHostPath)],
    ok = mnesia:delete({rabbit_vhost, VHostPath}),
    Fs1 ++ Fs2.

exists(VHostPath) ->
    mnesia:dirty_read({rabbit_vhost, VHostPath}) /= [].

list() ->
    mnesia:dirty_all_keys(rabbit_vhost).

-spec count() -> non_neg_integer().
count() ->
    length(list()).

-spec with(rabbit_types:vhost(), rabbit_misc:thunk(A)) -> A.

with(VHostPath, Thunk) ->
    fun () ->
            case mnesia:read({rabbit_vhost, VHostPath}) of
                [] ->
                    mnesia:abort({no_such_vhost, VHostPath});
                [_V] ->
                    Thunk()
            end
    end.

with_user_and_vhost(Username, VHostPath, Thunk) ->
    rabbit_misc:with_user(Username, with(VHostPath, Thunk)).

%% Like with/2 but outside an Mnesia tx
assert(VHostPath) -> case exists(VHostPath) of
                         true  -> ok;
                         false -> throw({error, {no_such_vhost, VHostPath}})
                     end.

update(VHostPath, Fun) ->
    case mnesia:read({rabbit_vhost, VHostPath}) of
        [] ->
            mnesia:abort({no_such_vhost, VHostPath});
        [V] ->
            V1 = Fun(V),
            ok = mnesia:write(rabbit_vhost, V1, write),
            V1
    end.

limits_of(VHostPath) when is_binary(VHostPath) ->
    assert(VHostPath),
    case mnesia:dirty_read({rabbit_vhost, VHostPath}) of
        [] ->
            mnesia:abort({no_such_vhost, VHostPath});
        [#vhost{limits = Limits}] ->
            Limits
    end;
limits_of(#vhost{virtual_host = Name}) ->
    limits_of(Name).

set_limits(VHost = #vhost{}, undefined) ->
    VHost#vhost{limits = undefined};
set_limits(VHost = #vhost{}, Limits) ->
    VHost#vhost{limits = Limits}.


dir(Vhost) ->
    <<Num:128>> = erlang:md5(Vhost),
    rabbit_misc:format("~.36B", [Num]).

msg_store_dir_path(VHost) ->
    EncodedName = dir(VHost),
    rabbit_data_coercion:to_list(filename:join([msg_store_dir_base(),
                                                EncodedName])).

msg_store_dir_wildcard() ->
    rabbit_data_coercion:to_list(filename:join([msg_store_dir_base(), "*"])).

msg_store_dir_base() ->
    Dir = rabbit_mnesia:dir(),
    filename:join([Dir, "msg_stores", "vhosts"]).

%%----------------------------------------------------------------------------

infos(Items, X) -> [{Item, i(Item, X)} || Item <- Items].

i(name,    VHost) -> VHost;
i(tracing, VHost) -> rabbit_trace:enabled(VHost);
i(cluster_state, VHost) -> vhost_cluster_state(VHost);
i(Item, _)        -> throw({bad_argument, Item}).

info(VHost)        -> infos(?INFO_KEYS, VHost).
info(VHost, Items) -> infos(Items, VHost).

info_all()      -> info_all(?INFO_KEYS).
info_all(Items) -> [info(VHost, Items) || VHost <- list()].

info_all(Ref, AggregatorPid)        -> info_all(?INFO_KEYS, Ref, AggregatorPid).
info_all(Items, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map(
       AggregatorPid, Ref, fun(VHost) -> info(VHost, Items) end, list()).
