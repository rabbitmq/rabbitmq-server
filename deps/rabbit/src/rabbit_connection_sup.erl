%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_connection_sup).

%% Supervisor for a (network) AMQP 0-9-1 client connection.
%%
%% Supervises
%%
%%  * rabbit_reader
%%  * Auxiliary process supervisor
%%
%% See also rabbit_reader, rabbit_connection_helper_sup.

-behaviour(supervisor).
-behaviour(ranch_protocol).

-export([start_link/3,
         reader/1,
         start_connection_helper_sup/2
        ]).

-export([init/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

%%----------------------------------------------------------------------------

-spec start_link(ranch:ref(), module(), any()) ->
    {'ok', pid(), pid()}.

start_link(Ref, _Transport, _Opts) ->
    {ok, SupPid} = supervisor:start_link(?MODULE, []),
    {ok, ReaderPid} =
        supervisor:start_child(
            SupPid,
            #{
                id => reader,
                start => {rabbit_reader, start_link, [Ref]},
                restart => transient,
                significant => true,
                shutdown => ?WORKER_WAIT,
                type => worker,
                modules => [rabbit_reader]
            }
        ),
    {ok, SupPid, ReaderPid}.

-spec reader(pid()) -> pid().

reader(Pid) ->
    hd(rabbit_misc:find_child(Pid, reader)).

-spec start_connection_helper_sup(pid(), supervisor:sup_flags()) ->
    supervisor:startchild_ret().
start_connection_helper_sup(ConnectionSupPid, ConnectionHelperSupFlags) ->
    supervisor:start_child(
      ConnectionSupPid,
      #{
        id => helper_sup,
        start => {rabbit_connection_helper_sup, start_link, [ConnectionHelperSupFlags]},
        restart => transient,
        significant => true,
        shutdown => infinity,
        type => supervisor
       }).

%%--------------------------------------------------------------------------

init([]) ->
    ?LG_PROCESS_TYPE(connection_sup),
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
