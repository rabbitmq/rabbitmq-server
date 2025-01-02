%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stomp_client_sup).
-behaviour(supervisor).
-behaviour(ranch_protocol).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([start_link/3, init/1]).

start_link(Ref, _Transport, Configuration) ->
    {ok, SupPid} = supervisor:start_link(?MODULE, []),
    ConnectionHelperSupFlags = #{strategy => one_for_one,
                                 intensity => 10,
                                 period => 10,
                                 auto_shutdown => any_significant},
    {ok, HelperPid} =
        supervisor:start_child(
            SupPid,
            #{
                id => rabbit_stomp_heartbeat_sup,
                start => {rabbit_connection_helper_sup, start_link, [ConnectionHelperSupFlags]},
                restart => transient,
                significant => true,
                shutdown => infinity,
                type => supervisor,
                modules => [rabbit_connection_helper_sup]
            }
        ),

    %% We want the reader to be transient since when it exits normally
    %% the processor may have some work still to do (and the reader
    %% tells the processor to exit). However, if the reader terminates
    %% abnormally then we want to take everything down.
    {ok, ReaderPid} = supervisor:start_child(
        SupPid,
        #{
            id => rabbit_stomp_reader,
            start => {rabbit_stomp_reader, start_link, [HelperPid, Ref, Configuration]},
            restart => transient,
            significant => true,
            shutdown => ?WORKER_WAIT,
            type => worker,
            modules => [rabbit_stomp_reader]
        }
    ),

    {ok, SupPid, ReaderPid}.

init([]) ->
    {ok,
        {
            #{
                strategy => one_for_all,
                intensity => 0,
                period => 1,
                auto_shutdown => any_significant
            },
            []
        }}.
