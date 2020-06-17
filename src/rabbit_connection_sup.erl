%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
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

-behaviour(supervisor2).
-behaviour(ranch_protocol).

-export([start_link/4, reader/1]).

-export([init/1]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-spec start_link(any(), rabbit_net:socket(), module(), any()) ->
          {'ok', pid(), pid()}.

start_link(Ref, _Sock, _Transport, _Opts) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
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
    {ok, HelperSup} =
        supervisor2:start_child(
          SupPid,
          {helper_sup, {rabbit_connection_helper_sup, start_link, []},
           intrinsic, infinity, supervisor, [rabbit_connection_helper_sup]}),
    {ok, ReaderPid} =
        supervisor2:start_child(
          SupPid,
          {reader, {rabbit_reader, start_link, [HelperSup, Ref]},
           intrinsic, ?WORKER_WAIT, worker, [rabbit_reader]}),
    {ok, SupPid, ReaderPid}.

-spec reader(pid()) -> pid().

reader(Pid) ->
    hd(supervisor2:find_child(Pid, reader)).

%%--------------------------------------------------------------------------

init([]) ->
    ?LG_PROCESS_TYPE(connection_sup),
    {ok, {{one_for_all, 0, 1}, []}}.
