%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_vhost_sup_sup).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/logger.hrl").

-behaviour(supervisor).

-export([init/1]).

-export([start_link/0, start/0]).
-export([init_vhost/1,
         start_vhost/1, start_vhost/2,
         get_vhost_sup/1, get_vhost_sup/2,
         save_vhost_sup/5,
         storage_id/1,
         id/1,
         save_vhost_process/2,
         save_vhost_recovery_terms/2,
         lookup_vhost_sup_record/1,
         lookup_vhost_recovery_terms/1]).
-export([delete_on_all_nodes/1, delete_on_all_nodes/2, start_on_all_nodes/1, start_on_all_nodes/2]).
-export([is_vhost_alive/1]).
-export([check/0]).

%% Internal
-export([stop_and_delete_vhost/1, stop_and_delete_vhost/2, restart_vhost/2]).

-record(vhost_sup, {vhost, vhost_sup_pid, wrapper_pid, vhost_process_pid, recovery_terms_pid, storage_id, id}).

start() ->
    case supervisor:start_child(rabbit_sup, {?MODULE,
                                             {?MODULE, start_link, []},
                                             permanent, infinity, supervisor,
                                             [?MODULE]}) of
        {ok, _}      -> ok;
        {error, Err} -> {error, Err}
    end.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% This assumes that a single vhost termination should not shut down nodes
    %% unless the operator opts in.
    RestartStrategy = vhost_restart_strategy(),
    _ = ets:new(?MODULE, [named_table, public, {keypos, #vhost_sup.vhost}]),
    {ok, {{simple_one_for_one, 0, 5},
          [{rabbit_vhost, {rabbit_vhost_sup_wrapper, start_link, []},
            RestartStrategy, ?SUPERVISOR_WAIT, supervisor,
            [rabbit_vhost_sup_wrapper, rabbit_vhost_sup]}]}}.

start_on_all_nodes(VHost) ->
    %% By default select only fully booted peers
    AllBooted = rabbit_nodes:list_running(),
    Nodes     = [node() | AllBooted],
    start_on_all_nodes(VHost, Nodes).

start_on_all_nodes(VHost, Nodes) ->
    Results   = [{Node, start_vhost(VHost, Node)} || Node <- Nodes],
    Failures  = lists:filter(fun
                                 ({_, {ok, _}}) -> false;
                                 ({_, {error, {already_started, _}}}) -> false;
                                 (_) -> true
                             end,
        Results),
    case Failures of
        []     -> ok;
        Errors -> {error, {failed_to_start_vhost_on_nodes, Errors}}
    end.

delete_on_all_nodes(VHost) ->
    delete_on_all_nodes(VHost, id(VHost)).

delete_on_all_nodes(VHost, Id) ->
    _ = [delete_on_node(VHost, Id, Node) || Node <- rabbit_nodes:list_running()],
    ok.

delete_on_node(VHost, Id, Node) ->
    case rabbit_misc:rpc_call(Node, ?MODULE, stop_and_delete_vhost, [VHost, Id]) of
        {badrpc, {'EXIT', {undef, _}}} when Id =:= undefined ->
            rabbit_misc:rpc_call(Node, ?MODULE, stop_and_delete_vhost, [VHost]);
        Result -> Result
    end.

stop_and_delete_vhost(VHost) ->
    stop_and_delete_vhost(VHost, id(VHost)).

stop_and_delete_vhost(VHost, Id) ->
    with_vhost(VHost, fun() ->
        Current = rabbit_db_vhost:get_id_for_deletion(VHost),
        case Current of
            {ok, Id} -> ok;
            {ok, New} when is_integer(Id), is_integer(New), New > Id ->
                delete_vhost(VHost, Id, Current);
            {ok, _} -> ok;
            {error, not_found} -> delete_vhost(VHost, Id, Current);
            {error, _} = Error -> Error
        end
    end).

delete_vhost(VHost, Id, Current) ->
    case lookup_vhost_sup_record(VHost) of
        #vhost_sup{id = Id, storage_id = StorageId} = Record ->
            ok = stop_vhost(Record),
            case {StorageId, rabbit_vhost_deletion:deleted_vhosts([{VHost, Id}])} of
                {Id, {ok, [{VHost, Id}]}} when is_integer(Id) ->
                    ok = rabbit_vhost:delete_storage(VHost, StorageId);
                _ -> ok
            end;
        #vhost_sup{id = undefined, storage_id = legacy} = Record when is_integer(Id) ->
            case rabbit_db_vhost:storage_id(Id) of
                {ok, legacy} -> stop_vhost(Record);
                _ -> ok
            end;
        _ -> ok
    end,
    ok = rabbit_vhost_deletion:vhost_deleted(VHost, Id),
    case Current of
        {ok, _} ->
            case do_start_vhost(VHost) of
                {ok, _} -> ok;
                {error, {already_started, _}} -> ok;
                {error, _} = Error -> Error
            end;
        _ -> ok
    end.

-spec restart_vhost(rabbit_types:vhost(), pid()) -> ok | {ok, pid()} | {error, term()}.
restart_vhost(VHost, Process) ->
    with_vhost(VHost, fun() ->
        case lookup_vhost_sup_record(VHost) of
            #vhost_sup{vhost_process_pid = Process, id = Id} = Record ->
                case rabbit_db_vhost:get_id_for_deletion(VHost) of
                    {ok, Id} -> ok;
                    {ok, New} when is_integer(New), Id =:= undefined orelse New > Id ->
                        replace_vhost(VHost, New, Record);
                    {ok, _} -> {error, stale_vhost_id};
                    {error, not_found} ->
                        delete_vhost(VHost, Id, {error, not_found});
                    {error, _} = Error -> Error
                end;
            _ -> ok
        end
    end).

stop_vhost(#vhost_sup{wrapper_pid = WrapperPid} = Record) ->
    case is_process_alive(WrapperPid) of
        true ->
            case supervisor:terminate_child(?MODULE, WrapperPid) of
                ok -> ok;
                {error, not_found} -> ok
            end;
        false -> ok
    end,
    true = ets:delete_object(?MODULE, Record),
    ok.

with_vhost(VHost, Fun) ->
    global:trans({{?MODULE, VHost}, self()}, Fun, [node()]).

-spec init_vhost(rabbit_types:vhost()) -> ok | {error, {no_such_vhost, rabbit_types:vhost()}}.
init_vhost(VHost) ->
    case start_vhost(VHost) of
        {ok, _} -> ok;
        {error, {already_started, _}} ->
            ?LOG_WARNING(
                "Attempting to start an already started vhost '~ts'.",
                [VHost]),
            ok;
        {error, {no_such_vhost, VHost}} ->
            {error, {no_such_vhost, VHost}};
        {error, Reason} ->
            case vhost_restart_strategy() of
                permanent ->
                    ?LOG_ERROR(
                        "Unable to initialize vhost data store for vhost '~ts'."
                        " Reason: ~tp",
                        [VHost, Reason]),
                    throw({error, Reason});
                transient ->
                    ?LOG_WARNING(
                        "Unable to initialize vhost data store for vhost '~ts'."
                        " The vhost will be stopped for this node. "
                        " Reason: ~tp",
                        [VHost, Reason]),
                    case metadata_unavailable(Reason) of
                        true ->
                            Sup = whereis(?MODULE),
                            spawn(fun() -> retry_vhost_start(VHost, Sup) end),
                            ok;
                        false -> ok
                    end
            end
    end.

retry_vhost_start(VHost, Sup) ->
    timer:sleep(5000),
    case whereis(?MODULE) of
        Sup when is_pid(Sup) ->
            case start_vhost(VHost) of
                {error, Reason} ->
                    case metadata_unavailable(Reason) of
                        true -> retry_vhost_start(VHost, Sup);
                        false -> ok
                    end;
                _ -> ok
            end;
        _ -> ok
    end.

metadata_unavailable(timeout) -> true;
metadata_unavailable(noproc) -> true;
metadata_unavailable({error, Reason}) -> metadata_unavailable(Reason);
metadata_unavailable({shutdown, Reason}) -> metadata_unavailable(Reason);
metadata_unavailable({failed_to_start_child, _, Reason}) -> metadata_unavailable(Reason);
metadata_unavailable(_) -> false.

-type vhost_error() :: {no_such_vhost, rabbit_types:vhost()} |
                       {vhost_supervisor_not_running, rabbit_types:vhost()}.

-spec get_vhost_sup(rabbit_types:vhost(), node()) -> {ok, pid()} | {error, vhost_error() | term()}.
get_vhost_sup(VHost, Node) ->
    case rabbit_misc:rpc_call(Node, rabbit_vhost_sup_sup, get_vhost_sup, [VHost]) of
        {ok, Pid} when is_pid(Pid) ->
            {ok, Pid};
        {error, Err} ->
            {error, Err};
        {badrpc, RpcErr} ->
            {error, RpcErr}
    end.

-spec get_vhost_sup(rabbit_types:vhost()) -> {ok, pid()} | {error, vhost_error()}.
get_vhost_sup(VHost) ->
    case rabbit_db_vhost:get(VHost) of
        undefined -> {error, {no_such_vhost, VHost}};
        V -> get_vhost_sup_for_id(VHost, vhost:get_id(V))
    end.

get_vhost_sup_for_id(VHost, Id) ->
    case lookup_vhost_sup_record(VHost) of
        #vhost_sup{id = undefined, storage_id = legacy} = Record when is_integer(Id) ->
            case rabbit_db_vhost:identity(VHost) of
                {ok, Id, legacy} -> adopt_legacy_vhost(Record, Id);
                _ -> ok
            end;
        _ -> ok
    end,
    case vhost_sup_pid(VHost, Id) of
        no_pid -> {error, {vhost_supervisor_not_running, VHost}};
        {ok, Pid} -> {ok, Pid}
    end.

-spec start_vhost(rabbit_types:vhost(), node()) -> {ok, pid()} | {error, term()}.
start_vhost(VHost, Node) ->
    case rabbit_misc:rpc_call(Node, rabbit_vhost_sup_sup, start_vhost, [VHost]) of
        {ok, Pid}        -> {ok, Pid};
        {error, Err}     -> {error, Err};
        {badrpc, RpcErr} -> {error, RpcErr}
    end.

-spec start_vhost(rabbit_types:vhost()) -> {ok, pid()} | {error, term()}.
start_vhost(VHost) ->
    with_vhost(VHost, fun() -> do_start_vhost(VHost) end).

do_start_vhost(VHost) ->
    case rabbit_vhost:id(VHost) of
        {ok, Id} -> start_vhost(VHost, Id, whereis(?MODULE));
        {error, feature_flag_disabled} -> start_vhost(VHost, undefined, whereis(?MODULE));
        {error, not_found} -> {error, {no_such_vhost, VHost}};
        {error, _} = Error -> Error
    end.

start_vhost(_VHost, _Id, undefined) ->
    {error, rabbit_vhost_sup_sup_not_running};
start_vhost(VHost, Id, _Supervisor) ->
    case lookup_vhost_sup_record(VHost) of
        #vhost_sup{id = Old} = Record when Old =/= Id ->
            case rabbit_db_vhost:get_id_for_deletion(VHost) of
                {ok, Id} when is_integer(Id), Old =:= undefined orelse Id > Old ->
                    replace_vhost(VHost, Id, Record);
                {ok, _} -> {error, stale_vhost_id};
                {error, _} = Error -> Error
            end;
        _ -> supervisor:start_child(?MODULE, [VHost, Id])
    end.

replace_vhost(VHost, Id, #vhost_sup{id = Old, storage_id = StorageId} = Record) ->
    case rabbit_db_vhost:identity(VHost) of
        {ok, Id, legacy} when Old =:= undefined, StorageId =:= legacy ->
            adopt_legacy_vhost(Record, Id),
            {ok, Record#vhost_sup.vhost_sup_pid};
        {ok, Id, _} ->
            ok = stop_vhost(Record),
            ok = rabbit_vhost_deletion:vhost_deleted(VHost, Old),
            supervisor:start_child(?MODULE, [VHost, Id]);
        {ok, _, _} -> {error, stale_vhost_id};
        {error, _} = Error -> Error
    end.

adopt_legacy_vhost(Record, Id) ->
    ets:select_replace(?MODULE, [{Record, [], [{const, Record#vhost_sup{id = Id}}]}]),
    ok.

-spec is_vhost_alive(rabbit_types:vhost()) -> boolean().
is_vhost_alive(VHost) ->
%% A vhost is considered alive if it's supervision tree is alive and
%% saved in the ETS table
    case lookup_vhost_sup_record(VHost) of
        #vhost_sup{wrapper_pid = WrapperPid,
                   vhost_sup_pid = VHostSupPid,
                   vhost_process_pid = VHostProcessPid}
                when is_pid(WrapperPid),
                     is_pid(VHostSupPid),
                     is_pid(VHostProcessPid) ->
            is_process_alive(WrapperPid)
            andalso
            is_process_alive(VHostSupPid)
            andalso
            is_process_alive(VHostProcessPid);
        _ -> false
    end.


-spec save_vhost_sup(rabbit_types:vhost(), vhost:id() | undefined, vhost:id() | legacy, pid(), pid()) -> ok.
save_vhost_sup(VHost, Id, StorageId, WrapperPid, VHostPid) ->
    true = ets:insert(?MODULE, #vhost_sup{vhost = VHost,
                                          vhost_sup_pid = VHostPid,
                                          wrapper_pid = WrapperPid,
                                          recovery_terms_pid = no_pid,
                                          storage_id = StorageId,
                                          id = Id}),
    ok.

-spec save_vhost_recovery_terms(rabbit_types:vhost(), pid()) -> ok.
save_vhost_recovery_terms(VHost, RecoveryTermsPid) ->
    true = ets:update_element(?MODULE, VHost,
                              [{#vhost_sup.recovery_terms_pid, RecoveryTermsPid}]),
    ok.

-spec save_vhost_process(rabbit_types:vhost(), pid()) -> ok.
save_vhost_process(VHost, VHostProcessPid) ->
    true = ets:update_element(?MODULE, VHost,
                              [{#vhost_sup.vhost_process_pid, VHostProcessPid}]),
    ok.

-spec id(rabbit_types:vhost()) -> vhost:id() | undefined.
id(VHost) ->
    case lookup_vhost_sup_record(VHost) of
        #vhost_sup{id = Id} -> Id;
        not_found -> undefined
    end.

-spec storage_id(rabbit_types:vhost()) -> {ok, vhost:id() | legacy} | not_found.
storage_id(VHost) ->
    case lookup_vhost_sup_record(VHost) of
        #vhost_sup{storage_id = StorageId} -> {ok, StorageId};
        not_found -> not_found
    end.

-spec lookup_vhost_sup_record(rabbit_types:vhost()) -> #vhost_sup{} | not_found.
lookup_vhost_sup_record(VHost) ->
    case ets:info(?MODULE, name) of
        ?MODULE ->
            case ets:lookup(?MODULE, VHost) of
                [] -> not_found;
                [#vhost_sup{} = VHostSup] -> VHostSup
            end;
        undefined -> not_found
    end.

-spec lookup_vhost_recovery_terms(rabbit_types:vhost()) -> pid() | not_found.
lookup_vhost_recovery_terms(VHost) ->
    case ets:info(?MODULE, name) of
        ?MODULE ->
            case ets:lookup(?MODULE, VHost) of
                [] -> not_found;
                [#vhost_sup{} = VHostSup] -> VHostSup#vhost_sup.recovery_terms_pid
            end;
        undefined -> not_found
    end.

-spec vhost_sup_pid(rabbit_types:vhost(), vhost:id() | undefined) -> no_pid | {ok, pid()}.
vhost_sup_pid(VHost, Id) ->
    case lookup_vhost_sup_record(VHost) of
        #vhost_sup{id = Id, vhost_sup_pid = Pid} = VHostSup ->
            case erlang:is_process_alive(Pid) of
                true  -> {ok, Pid};
                false ->
                    ets:delete_object(?MODULE, VHostSup),
                    no_pid
            end;
        _ -> no_pid
    end.

vhost_restart_strategy() ->
    %% This assumes that a single vhost termination should not shut down nodes
    %% unless the operator opts in.
    case application:get_env(rabbit, vhost_restart_strategy, continue) of
        continue  -> transient;
        stop_node -> permanent;
        transient -> transient;
        permanent -> permanent
    end.

check() ->
    VHosts = rabbit_vhost:list_names(),
    lists:filter(
      fun(V) ->
              case get_vhost_sup(V) of
                  {ok, Sup} ->
                      MsgStores = [Pid || {Name, Pid, _, _} <- supervisor:which_children(Sup),
                                         lists:member(Name, [msg_store_persistent,
                                                             msg_store_transient])],
                      not is_vhost_alive(V) orelse (not lists:all(fun(P) ->
                                                                          erlang:is_process_alive(P)
                                                                  end, MsgStores));
                  {error, _} ->
                      true
              end
      end, VHosts).
