%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_vhost_sup_sup).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(supervisor).

-export([init/1]).

-export([start_link/0, start/0]).
-export([init_vhost/1,
         start_vhost/1, start_vhost/2,
         get_vhost_sup/1, get_vhost_sup/2,
         save_vhost_sup/3,
         save_vhost_process/2,
         save_vhost_recovery_terms/2,
         lookup_vhost_sup_record/1,
         lookup_vhost_recovery_terms/1]).
-export([delete_on_all_nodes/1, start_on_all_nodes/1, start_on_all_nodes/2]).
-export([is_vhost_alive/1]).
-export([check/0]).

%% Internal
-export([stop_and_delete_vhost/1]).

-record(vhost_sup, {vhost, vhost_sup_pid, wrapper_pid, vhost_process_pid, recovery_terms_pid}).

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
    _ = [ stop_and_delete_vhost(VHost, Node) || Node <- rabbit_nodes:list_running() ],
    ok.

stop_and_delete_vhost(VHost) ->
    StopResult = case lookup_vhost_sup_record(VHost) of
        not_found ->
            rabbit_log:warning("Supervisor for vhost '~ts' not found during deletion procedure",
                            [VHost]),
            ok;
        #vhost_sup{wrapper_pid = WrapperPid,
                   vhost_sup_pid = VHostSupPid} ->
            case is_process_alive(WrapperPid) of
                false ->
                    rabbit_log:info("Supervisor ~tp for vhost '~ts' already stopped",
                                    [VHostSupPid, VHost]),
                    ok;
                true  ->
                    rabbit_log:info("Stopping vhost supervisor ~tp"
                                    " for vhost '~ts'",
                                    [VHostSupPid, VHost]),
                    case supervisor:terminate_child(?MODULE, WrapperPid) of
                        ok ->
                            true = ets:delete(?MODULE, VHost),
                            ok;
                        Other ->
                            Other
                    end
            end
    end,
    ok = rabbit_vhost:delete_storage(VHost),
    StopResult.

%% We take an optimistic approach whan stopping a remote VHost supervisor.
stop_and_delete_vhost(VHost, Node) when Node == node(self()) ->
    stop_and_delete_vhost(VHost);
stop_and_delete_vhost(VHost, Node) ->
    case rabbit_misc:rpc_call(Node, rabbit_vhost_sup_sup, stop_and_delete_vhost, [VHost]) of
        ok -> ok;
        {badrpc, RpcErr} ->
            rabbit_log:error("Failed to stop and delete a vhost ~tp"
                             " on node ~tp."
                             " Reason: ~tp",
                             [VHost, Node, RpcErr]),
            {error, RpcErr}
    end.

-spec init_vhost(rabbit_types:vhost()) -> ok | {error, {no_such_vhost, rabbit_types:vhost()}}.
init_vhost(VHost) ->
    case start_vhost(VHost) of
        {ok, _} -> ok;
        {error, {already_started, _}} ->
            rabbit_log:warning(
                "Attempting to start an already started vhost '~ts'.",
                [VHost]),
            ok;
        {error, {no_such_vhost, VHost}} ->
            {error, {no_such_vhost, VHost}};
        {error, Reason} ->
            case vhost_restart_strategy() of
                permanent ->
                    rabbit_log:error(
                        "Unable to initialize vhost data store for vhost '~ts'."
                        " Reason: ~tp",
                        [VHost, Reason]),
                    throw({error, Reason});
                transient ->
                    rabbit_log:warning(
                        "Unable to initialize vhost data store for vhost '~ts'."
                        " The vhost will be stopped for this node. "
                        " Reason: ~tp",
                        [VHost, Reason]),
                    ok
            end
    end.

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
    case rabbit_vhost:exists(VHost) of
        false ->
            {error, {no_such_vhost, VHost}};
        true ->
            case vhost_sup_pid(VHost) of
                no_pid ->
                    {error, {vhost_supervisor_not_running, VHost}};
                {ok, Pid} when is_pid(Pid) ->
                    {ok, Pid}
            end
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
    case rabbit_vhost:exists(VHost) of
        false -> {error, {no_such_vhost, VHost}};
        true  ->
            case whereis(?MODULE) of
                Pid when is_pid(Pid) ->
                    supervisor:start_child(?MODULE, [VHost]);
                undefined ->
                    {error, rabbit_vhost_sup_sup_not_running}
            end
    end.

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


-spec save_vhost_sup(rabbit_types:vhost(), pid(), pid()) -> ok.
save_vhost_sup(VHost, WrapperPid, VHostPid) ->
    true = ets:insert(?MODULE, #vhost_sup{vhost = VHost,
                                          vhost_sup_pid = VHostPid,
                                          wrapper_pid = WrapperPid,
                                          recovery_terms_pid = no_pid}),
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

-spec vhost_sup_pid(rabbit_types:vhost()) -> no_pid | {ok, pid()}.
vhost_sup_pid(VHost) ->
    case lookup_vhost_sup_record(VHost) of
        not_found ->
            no_pid;
        #vhost_sup{vhost_sup_pid = Pid} = VHostSup ->
            case erlang:is_process_alive(Pid) of
                true  -> {ok, Pid};
                false ->
                    ets:delete_object(?MODULE, VHostSup),
                    no_pid
            end
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
