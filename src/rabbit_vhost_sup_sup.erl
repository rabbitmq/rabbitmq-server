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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_vhost_sup_sup).

-include("rabbit.hrl").

-behaviour(supervisor2).

-export([init/1]).

-export([start_link/0, start/0]).
-export([vhost_sup/1, vhost_sup/2, save_vhost_sup/3]).
-export([delete_on_all_nodes/1]).
-export([start_on_all_nodes/1]).

-export([save_vhost_process/2]).
-export([is_vhost_alive/1]).

%% Internal
-export([stop_and_delete_vhost/1]).

-record(vhost_sup, {vhost, vhost_sup_pid, wrapper_pid, vhost_process_pid}).

start() ->
    case supervisor:start_child(rabbit_sup, {?MODULE,
                                             {?MODULE, start_link, []},
                                             permanent, infinity, supervisor,
                                             [?MODULE]}) of
        {ok, _}      -> ok;
        {error, Err} -> {error, Err}
    end.

start_link() ->
    supervisor2:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% This assumes that a single vhost termination should not shut down nodes
    %% unless the operator opts in.
    RestartStrategy = vhost_restart_strategy(),
    ets:new(?MODULE, [named_table, public, {keypos, #vhost_sup.vhost}]),

    {ok, {{simple_one_for_one, 0, 5},
          [{rabbit_vhost, {rabbit_vhost_sup_wrapper, start_link, []},
            RestartStrategy, ?SUPERVISOR_WAIT, supervisor,
            [rabbit_vhost_sup_wrapper, rabbit_vhost_sup]}]}}.

start_on_all_nodes(VHost) ->
    [ {ok, _} = vhost_sup(VHost, Node) || Node <- rabbit_nodes:all_running() ],
    ok.

delete_on_all_nodes(VHost) ->
    [ stop_and_delete_vhost(VHost, Node) || Node <- rabbit_nodes:all_running() ],
    ok.

stop_and_delete_vhost(VHost) ->
    case get_vhost_sup(VHost) of
        not_found -> ok;
        #vhost_sup{wrapper_pid = WrapperPid,
                   vhost_sup_pid = VHostSupPid} ->
            case is_process_alive(WrapperPid) of
                false -> ok;
                true  ->
                    rabbit_log:info("Stopping vhost supervisor ~p"
                                    " for vhost '~s'~n",
                                    [VHostSupPid, VHost]),
                    case supervisor2:terminate_child(?MODULE, WrapperPid) of
                        ok ->
                            ets:delete(?MODULE, VHost),
                            ok = rabbit_vhost:delete_storage(VHost);
                        Other ->
                            Other
                    end
            end
    end.

%% We take an optimistic approach whan stopping a remote VHost supervisor.
stop_and_delete_vhost(VHost, Node) when Node == node(self()) ->
    stop_and_delete_vhost(VHost);
stop_and_delete_vhost(VHost, Node) ->
    case rabbit_misc:rpc_call(Node, rabbit_vhost_sup_sup, stop_and_delete_vhost, [VHost]) of
        ok -> ok;
        {badrpc, RpcErr} ->
            rabbit_log:error("Failed to stop and delete a vhost ~p"
                             " on node ~p."
                             " Reason: ~p",
                             [VHost, Node, RpcErr]),
            {error, RpcErr}
    end.

-spec vhost_sup(rabbit_types:vhost(), node()) -> {ok, pid()} | {error, {no_such_vhost, rabbit_types:vhost()} | term()}.
vhost_sup(VHost, Local) when Local == node(self()) ->
    vhost_sup(VHost);
vhost_sup(VHost, Node) ->
    case rabbit_misc:rpc_call(Node, rabbit_vhost_sup_sup, vhost_sup, [VHost]) of
        {ok, Pid} when is_pid(Pid) ->
            {ok, Pid};
        {badrpc, RpcErr} ->
            {error, RpcErr}
    end.

-spec vhost_sup(rabbit_types:vhost()) -> {ok, pid()} | {error, {no_such_vhost, rabbit_types:vhost()}}.
vhost_sup(VHost) ->
    case rabbit_vhost:exists(VHost) of
        false -> {error, {no_such_vhost, VHost}};
        true  ->
            case vhost_sup_pid(VHost) of
                no_pid ->
                    case supervisor2:start_child(?MODULE, [VHost]) of
                        {ok, _}                       -> ok;
                        {error, {already_started, _}} -> ok;
                        Error                         -> throw(Error)
                    end,
                    {ok, _} = vhost_sup_pid(VHost);
                {ok, Pid} when is_pid(Pid) ->
                    {ok, Pid}
            end
    end.


-spec is_vhost_alive(rabbit_types:vhost()) -> boolean().
is_vhost_alive(VHost) ->
%% A vhost is considered alive if it's supervision tree is alive and
%% saved in the ETS table
    case get_vhost_sup(VHost) of
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
                                          wrapper_pid = WrapperPid}),
    ok.

-spec save_vhost_process(rabbit_types:vhost(), pid()) -> ok.
save_vhost_process(VHost, VHostProcessPid) ->
    true = ets:update_element(?MODULE, VHost,
                              {#vhost_sup.vhost_process_pid, VHostProcessPid}),
    ok.

-spec get_vhost_sup(rabbit_types:vhost()) -> #vhost_sup{}.
get_vhost_sup(VHost) ->
    case ets:lookup(?MODULE, VHost) of
        [] -> not_found;
        [#vhost_sup{} = VHostSup] -> VHostSup
    end.

-spec vhost_sup_pid(rabbit_types:vhost()) -> no_pid | {ok, pid()}.
vhost_sup_pid(VHost) ->
    case get_vhost_sup(VHost) of
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
