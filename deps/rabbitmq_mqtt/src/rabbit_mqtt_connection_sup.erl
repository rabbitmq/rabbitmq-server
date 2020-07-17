%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_connection_sup).

-behaviour(supervisor).
-behaviour(ranch_protocol).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([start_link/4, start_keepalive_link/0]).

-export([init/1]).

%%----------------------------------------------------------------------------

start_link(Ref, _Sock, _Transport, []) ->
    {ok, SupPid} = supervisor:start_link(?MODULE, []),
    {ok, KeepaliveSup} = supervisor:start_child(SupPid,
                          #{
                              id       => rabbit_mqtt_keepalive_sup,
                              start    => {rabbit_mqtt_connection_sup, start_keepalive_link, []},
                              restart  => transient,
                              shutdown => infinity,
                              type     => supervisor,
                              modules  => [rabbit_keepalive_sup]
                          }),
    {ok, ReaderPid} = supervisor:start_child(SupPid,
                        #{
                            id       => rabbit_mqtt_reader,
                            start    => {rabbit_mqtt_reader, start_link, [KeepaliveSup, Ref]},
                            restart  => transient,
                            shutdown => ?WORKER_WAIT,
                            type     => worker,
                            modules  => [rabbit_mqtt_reader]
                        }),
    {ok, SupPid, ReaderPid}.

start_keepalive_link() ->
    supervisor:start_link(?MODULE, []).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.


