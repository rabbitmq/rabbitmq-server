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
-export([start_vhost/1, start_vhost/2, start_on_all_nodes/1]).

%% Internal
-export([stop_and_delete_vhost/1]).

-record(vhost_sup, {vhost, vhost_sup_pid, wrapper_pid}).

start() ->
    rabbit_sup:start_supervisor_child(?MODULE).

start_link() ->
    supervisor2:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ets:new(?MODULE, [named_table, public, {keypos, #vhost_sup.vhost}]),
    {ok, {{simple_one_for_one, 0, 5},
          [{rabbit_vhost, {rabbit_vhost_sup_wrapper, start_link, []},
            permanent, infinity, supervisor,
            [rabbit_vhost_sup_wrapper, rabbit_vhost_sup]}]}}.

start_on_all_nodes(VHost) ->
    [ {ok, _} = start_vhost(VHost, Node) || Node <- rabbit_nodes:all_running() ],
    ok.

delete_on_all_nodes(VHost) ->
    [ stop_and_delete_vhost(VHost, Node) || Node <- rabbit_nodes:all_running() ],
    ok.

start_vhost(VHost, Node) when Node == node(self()) ->
    start_vhost(VHost);
start_vhost(VHost, Node) ->
    case rabbit_misc:rpc_call(Node, rabbit_vhost_sup_sup, start_vhost, [VHost]) of
        {ok, Pid} when is_pid(Pid) ->
            {ok, Pid};
        {badrpc, RpcErr} ->
            {error, RpcErr}
    end.

start_vhost(VHost) ->
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

stop_and_delete_vhost(VHost) ->
    case get_vhost_sup(VHost) of
        not_found -> ok;
        #vhost_sup{wrapper_pid = WrapperPid,
                   vhost_sup_pid = VHostSupPid} = VHostSup ->
            case is_process_alive(WrapperPid) of
                false -> ok;
                true  ->
                    rabbit_log:info("Stopping vhost supervisor ~p"
                                    " for vhost ~p~n",
                                    [VHostSupPid, VHost]),
                    case supervisor2:terminate_child(?MODULE, WrapperPid) of
                        ok ->
                            ets:delete_object(?MODULE, VHostSup),
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

-spec vhost_sup(rabbit_types:vhost(), node()) -> {ok, pid()}.
vhost_sup(VHost, Local) when Local == node(self()) ->
    vhost_sup(VHost);
vhost_sup(VHost, Node) ->
    case rabbit_misc:rpc_call(Node, rabbit_vhost_sup_sup, vhost_sup, [VHost]) of
        {ok, Pid} when is_pid(Pid) ->
            {ok, Pid};
        {badrpc, RpcErr} ->
            {error, RpcErr}
    end.

-spec vhost_sup(rabbit_types:vhost()) -> {ok, pid()}.
vhost_sup(VHost) ->
    start_vhost(VHost).

-spec save_vhost_sup(rabbit_types:vhost(), pid(), pid()) -> ok.
save_vhost_sup(VHost, WrapperPid, VHostPid) ->
    true = ets:insert(?MODULE, #vhost_sup{vhost = VHost,
                                          vhost_sup_pid = VHostPid,
                                          wrapper_pid = WrapperPid}),
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

