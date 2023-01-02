%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqqueue_sup_sup).

-behaviour(supervisor).

-export([start_link/0, start_queue_process/3]).
-export([start_for_vhost/1, stop_for_vhost/1,
         find_for_vhost/2, find_for_vhost/1]).

-export([init/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

-define(SERVER, ?MODULE).

%%----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().

start_link() ->
    supervisor:start_link(?MODULE, []).

-spec start_queue_process
        (node(), amqqueue:amqqueue(), 'declare' | 'recovery' | 'slave') ->
            pid().

start_queue_process(Node, Q, StartMode) ->
    #resource{virtual_host = VHost} = amqqueue:get_name(Q),
    {ok, Sup} = find_for_vhost(VHost, Node),
    {ok, _SupPid, QPid} = supervisor:start_child(Sup, [Q, StartMode]),
    QPid.

init([]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 10,
                 period => 10},
    ChildSpec = #{id => rabbit_amqqueue_sup,
                  start => {rabbit_amqqueue_sup, start_link, []},
                  restart => temporary,
                  shutdown => ?SUPERVISOR_WAIT,
                  type => supervisor,
                  modules => [rabbit_amqqueue_sup]},
    {ok, {SupFlags, [ChildSpec]}}.

-spec find_for_vhost(rabbit_types:vhost()) -> {ok, pid()} | {error, term()}.
find_for_vhost(VHost) ->
    find_for_vhost(VHost, node()).

-spec find_for_vhost(rabbit_types:vhost(), atom()) -> {ok, pid()} | {error, term()}.
find_for_vhost(VHost, Node) ->
    {ok, VHostSup} = rabbit_vhost_sup_sup:get_vhost_sup(VHost, Node),
    case rabbit_misc:find_child(VHostSup, rabbit_amqqueue_sup_sup) of
        [QSup] -> {ok, QSup};
        Result -> {error, {queue_supervisor_not_found, Result}}
    end.

-spec start_for_vhost(rabbit_types:vhost()) -> {ok, pid()} | {error, term()}.
start_for_vhost(VHost) ->
    case rabbit_vhost_sup_sup:get_vhost_sup(VHost) of
        {ok, VHostSup} ->
            StartMFA = {rabbit_amqqueue_sup_sup, start_link, []},
            ChildSpec = #{id => rabbit_amqqueue_sup_sup,
                          start => StartMFA,
                          restart => transient,
                          shutdown => infinity,
                          type => supervisor,
                          modules => [rabbit_amqqueue_sup_sup]},
            supervisor:start_child(VHostSup, ChildSpec);
        %% we can get here if a vhost is added and removed concurrently
        %% e.g. some integration tests do it
        {error, {no_such_vhost, VHost}} ->
            rabbit_log:error("Failed to start a queue process supervisor for vhost ~ts: vhost no longer exists!",
                             [VHost]),
            {error, {no_such_vhost, VHost}}
    end.

-spec stop_for_vhost(rabbit_types:vhost()) -> ok.
stop_for_vhost(VHost) ->
    case rabbit_vhost_sup_sup:get_vhost_sup(VHost) of
        {ok, VHostSup} ->
            ok = supervisor:terminate_child(VHostSup, rabbit_amqqueue_sup_sup),
            ok = supervisor:delete_child(VHostSup, rabbit_amqqueue_sup_sup);
        %% see start/1
        {error, {no_such_vhost, VHost}} ->
            rabbit_log:error("Failed to stop a queue process supervisor for vhost ~ts: vhost no longer exists!",
                             [VHost]),
            ok
    end.
