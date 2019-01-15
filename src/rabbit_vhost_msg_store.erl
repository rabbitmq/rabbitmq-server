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

-module(rabbit_vhost_msg_store).

-include("rabbit.hrl").

-export([start/4, stop/2, client_init/5, successfully_recovered_state/2]).
-export([vhost_store_pid/2]).

start(VHost, Type, ClientRefs, StartupFunState) when is_list(ClientRefs);
                                                     ClientRefs == undefined  ->
    case rabbit_vhost_sup_sup:get_vhost_sup(VHost) of
        {ok, VHostSup} ->
            VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
            supervisor2:start_child(VHostSup,
                                    {Type, {rabbit_msg_store, start_link,
                                            [Type, VHostDir, ClientRefs, StartupFunState]},
                                     transient, ?WORKER_WAIT, worker, [rabbit_msg_store]});
        %% we can get here if a vhost is added and removed concurrently
        %% e.g. some integration tests do it
        {error, {no_such_vhost, VHost}} = E ->
            rabbit_log:error("Failed to start a message store for vhost ~s: vhost no longer exists!",
                             [VHost]),
            E
    end.

stop(VHost, Type) ->
    case rabbit_vhost_sup_sup:get_vhost_sup(VHost) of
        {ok, VHostSup} ->
            ok = supervisor2:terminate_child(VHostSup, Type),
            ok = supervisor2:delete_child(VHostSup, Type);
        %% see start/4
        {error, {no_such_vhost, VHost}} ->
            rabbit_log:error("Failed to stop a message store for vhost ~s: vhost no longer exists!",
                             [VHost]),

            ok
    end.

client_init(VHost, Type, Ref, MsgOnDiskFun, CloseFDsFun) ->
    with_vhost_store(VHost, Type, fun(StorePid) ->
        rabbit_msg_store:client_init(StorePid, Ref, MsgOnDiskFun, CloseFDsFun)
    end).

with_vhost_store(VHost, Type, Fun) ->
    case vhost_store_pid(VHost, Type) of
        no_pid ->
            throw({message_store_not_started, Type, VHost});
        Pid when is_pid(Pid) ->
            Fun(Pid)
    end.

vhost_store_pid(VHost, Type) ->
    {ok, VHostSup} = rabbit_vhost_sup_sup:get_vhost_sup(VHost),
    case supervisor2:find_child(VHostSup, Type) of
        [Pid] -> Pid;
        []    -> no_pid
    end.

successfully_recovered_state(VHost, Type) ->
    with_vhost_store(VHost, Type, fun(StorePid) ->
        rabbit_msg_store:successfully_recovered_state(StorePid)
    end).
