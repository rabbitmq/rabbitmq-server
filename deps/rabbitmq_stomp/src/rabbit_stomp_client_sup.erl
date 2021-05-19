%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stomp_client_sup).
-behaviour(supervisor2).
-behaviour(ranch_protocol).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([start_link/3, init/1]).

start_link(Ref, _Transport, Configuration) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    {ok, HelperPid} =
        supervisor2:start_child(SupPid,
                                {rabbit_stomp_heartbeat_sup,
                                 {rabbit_connection_helper_sup, start_link, []},
                                 intrinsic, infinity, supervisor,
                                 [rabbit_connection_helper_sup]}),

    %% We want the reader to be transient since when it exits normally
    %% the processor may have some work still to do (and the reader
    %% tells the processor to exit). However, if the reader terminates
    %% abnormally then we want to take everything down.
    {ok, ReaderPid} = supervisor2:start_child(
                        SupPid,
                        {rabbit_stomp_reader,
                         {rabbit_stomp_reader,
                          start_link, [HelperPid, Ref, Configuration]},
                         intrinsic, ?WORKER_WAIT, worker,
                         [rabbit_stomp_reader]}),

    {ok, SupPid, ReaderPid}.

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.

