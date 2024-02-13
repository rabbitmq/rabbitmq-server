%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_connection_sup).

%% Supervisor for a (network) AMQP client connection.
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
         remove_connection_helper_sup/2
        ]).

-export([init/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

%%----------------------------------------------------------------------------

-spec start_link(ranch:ref(), module(), any()) ->
    {'ok', pid(), pid()}.

start_link(Ref, _Transport, _Opts) ->
    {ok, SupPid} = supervisor:start_link(?MODULE, []),
    %% We need to get channels in the hierarchy here so they get shut
    %% down after the reader, so the reader gets a chance to terminate
    %% them cleanly. But for 1.0 readers we can't start the real
    %% ch_sup_sup (because we don't know if we will be 0-9-1 or 1.0) -
    %% so we add another supervisor into the hierarchy.
    %%
    %% This supervisor also acts as an intermediary for heartbeaters and
    %% the queue collector process, since these must not be siblings of the
    %% reader due to the potential for deadlock if they are added/restarted
    %% whilst the supervision tree is shutting down.
    ChildSpec = #{restart => transient,
                  significant => true,
                  shutdown => infinity,
                  type => supervisor},
    {ok, HelperSup091} =
        supervisor:start_child(
          SupPid,
          ChildSpec#{
            id => helper_sup_amqp_091,
            start => {rabbit_connection_helper_sup, start_link,
                      [#{strategy => one_for_one,
                         intensity => 10,
                         period => 10,
                         auto_shutdown => any_significant}]}}
         ),
    {ok, HelperSup10} =
        supervisor:start_child(
          SupPid,
          ChildSpec#{
            id => helper_sup_amqp_10,
            start => {rabbit_connection_helper_sup, start_link,
                      [#{strategy => one_for_all,
                         intensity => 0,
                         period => 1,
                         auto_shutdown => any_significant}]}}
         ),
    {ok, ReaderPid} =
        supervisor:start_child(
            SupPid,
            #{
                id => reader,
                start => {rabbit_reader, start_link, [{HelperSup091, HelperSup10}, Ref]},
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

-spec remove_connection_helper_sup(pid(), helper_sup_amqp_091 | helper_sup_amqp_10) -> ok.
remove_connection_helper_sup(ConnectionSupPid, ConnectionHelperId) ->
    ok = supervisor:terminate_child(ConnectionSupPid, ConnectionHelperId),
    ok = supervisor:delete_child(ConnectionSupPid, ConnectionHelperId).

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
