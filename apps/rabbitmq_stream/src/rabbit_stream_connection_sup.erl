%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_connection_sup).

-behaviour(supervisor2).
-behaviour(ranch_protocol).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([start_link/3,
         start_keepalive_link/0]).
-export([init/1]).

start_link(Ref, Transport, Opts) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    {ok, KeepaliveSup} =
        supervisor2:start_child(SupPid,
                                {rabbit_stream_keepalive_sup,
                                 {rabbit_stream_connection_sup,
                                  start_keepalive_link, []},
                                 intrinsic,
                                 infinity,
                                 supervisor,
                                 [rabbit_keepalive_sup]}),
    {ok, ReaderPid} =
        supervisor2:start_child(SupPid,
                                {rabbit_stream_reader,
                                 {rabbit_stream_reader, start_link,
                                  [KeepaliveSup, Transport, Ref, Opts]},
                                 intrinsic,
                                 ?WORKER_WAIT,
                                 worker,
                                 [rabbit_stream_reader]}),
    {ok, SupPid, ReaderPid}.

start_keepalive_link() ->
    supervisor2:start_link(?MODULE, []).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
