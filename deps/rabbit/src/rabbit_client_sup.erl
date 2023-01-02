%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_client_sup).

-behaviour(supervisor).

-export([start_link/1, start_link/2, start_link_worker/2]).

-export([init/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

%%----------------------------------------------------------------------------

-spec start_link(rabbit_types:mfargs()) ->
          rabbit_types:ok_pid_or_error().

start_link(Callback) ->
    supervisor:start_link(?MODULE, Callback).

-spec start_link({'local', atom()}, rabbit_types:mfargs()) ->
          rabbit_types:ok_pid_or_error().

start_link(SupName, Callback) ->
    supervisor:start_link(SupName, ?MODULE, Callback).

-spec start_link_worker({'local', atom()}, rabbit_types:mfargs()) ->
          rabbit_types:ok_pid_or_error().

start_link_worker(SupName, Callback) ->
    supervisor:start_link(SupName, ?MODULE, {Callback, worker}).

init({M, F, A}) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 0,
                 period => 1},
    ChildSpec = #{id => client,
                  start => {M, F, A},
                  restart => temporary,
                  shutdown => infinity,
                  type => supervisor,
                  modules => [M]},
    {ok, {SupFlags, [ChildSpec]}};
init({{M, F, A}, worker}) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 0,
                 period => 1},
    ChildSpec = #{id => client,
                start => {M, F, A},
                restart => temporary,
                shutdown => ?WORKER_WAIT,
                type => worker,
                modules => [M]},
    {ok, {SupFlags, [ChildSpec]}}.
