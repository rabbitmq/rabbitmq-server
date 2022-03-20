%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_client_sup).

-behaviour(supervisor2).

-export([start_link/1, start_link/2, start_link_worker/2]).

-export([init/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

%%----------------------------------------------------------------------------

-spec start_link(rabbit_types:mfargs()) ->
          rabbit_types:ok_pid_or_error().

start_link(Callback) ->
    supervisor2:start_link(?MODULE, Callback).

-spec start_link({'local', atom()}, rabbit_types:mfargs()) ->
          rabbit_types:ok_pid_or_error().

start_link(SupName, Callback) ->
    supervisor2:start_link(SupName, ?MODULE, Callback).

-spec start_link_worker({'local', atom()}, rabbit_types:mfargs()) ->
          rabbit_types:ok_pid_or_error().

start_link_worker(SupName, Callback) ->
    supervisor2:start_link(SupName, ?MODULE, {Callback, worker}).

init({M,F,A}) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{client, {M,F,A}, temporary, infinity, supervisor, [M]}]}};
init({{M,F,A}, worker}) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{client, {M,F,A}, temporary, ?WORKER_WAIT, worker, [M]}]}}.
