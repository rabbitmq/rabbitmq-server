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
-export([vhost_sup/1, vhost_sup/2]).
-export([start_vhost/1, stop_and_delete_vhost/1, delete_on_all_nodes/1]).

start() ->
    rabbit_sup:start_supervisor_child(?MODULE).

start_link() ->
    supervisor2:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ets:new(?MODULE, [named_table, public]),
    {ok, {{simple_one_for_one, 1, 5},
          [{rabbit_vhost, {rabbit_vhost_sup_sup, start_vhost, []},
            transient, infinity, supervisor,
            [rabbit_vhost_sup_sup, rabbit_vhost_sup]}]}}.

start_vhost(VHost) ->
    case rabbit_vhost_sup:start_link(VHost) of
        {ok, Pid} ->
            ok = save_vhost_pid(VHost, Pid),
            ok = rabbit_vhost:recover(VHost),
            {ok, Pid};
        Other ->
            Other
    end.

stop_and_delete_vhost(VHost) ->
    case vhost_pid(VHost) of
        no_pid -> ok;
        Pid when is_pid(Pid) ->
            rabbit_log:info("Stopping vhost supervisor ~p for vhost ~p~n",
                            [Pid, VHost]),
            case supervisor2:terminate_child(?MODULE, Pid) of
                ok ->
                    ok = rabbit_vhost:delete_storage(VHost);
                Other ->
                    Other
            end
    end.

delete_on_all_nodes(VHost) ->
    [ stop_and_delete_vhost(VHost, Node) || Node <- rabbit_nodes:all_running() ],
    ok.

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
    case rabbit_vhost:exists(VHost) of
        false -> {error, {no_such_vhost, VHost}};
        true  ->
            case vhost_pid(VHost) of
                no_pid ->
                    case supervisor2:start_child(?MODULE, [VHost]) of
                        {ok, Pid}                       -> {ok, Pid};
                        {error, {already_started, Pid}} -> {ok, Pid};
                        Error                           -> throw(Error)
                    end;
                Pid when is_pid(Pid) ->
                    {ok, Pid}
            end
    end.

save_vhost_pid(VHost, Pid) ->
    true = ets:insert(?MODULE, {VHost, Pid}),
    ok.

-spec vhost_pid(rabbit_types:vhost()) -> no_pid | pid().
vhost_pid(VHost) ->
    case ets:lookup(?MODULE, VHost) of
        []    -> no_pid;
        [{VHost, Pid}] ->
            case erlang:is_process_alive(Pid) of
                true  -> Pid;
                false ->
                    ets:delete_object(?MODULE, {VHost, Pid}),
                    no_pid
            end
    end.
