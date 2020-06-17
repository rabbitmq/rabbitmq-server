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

-module(rabbit_client_sup).

-behaviour(supervisor2).

-export([start_link/1, start_link/2, start_link_worker/2]).

-export([init/1]).

-include("rabbit.hrl").

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
