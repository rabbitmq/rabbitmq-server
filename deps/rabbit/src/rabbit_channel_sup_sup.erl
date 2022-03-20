%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_channel_sup_sup).

%% Supervisor for AMQP 0-9-1 channels. Every AMQP 0-9-1 connection has
%% one of these.
%%
%% See also rabbit_channel_sup, rabbit_connection_helper_sup, rabbit_reader.

-behaviour(supervisor2).

-export([start_link/0, start_channel/2]).

-export([init/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

%%----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().

start_link() ->
    supervisor2:start_link(?MODULE, []).

-spec start_channel(pid(), rabbit_channel_sup:start_link_args()) ->
          {'ok', pid(), {pid(), any()}}.

start_channel(Pid, Args) ->
    supervisor2:start_child(Pid, [Args]).

%%----------------------------------------------------------------------------

init([]) ->
    ?LG_PROCESS_TYPE(channel_sup_sup),
    {ok, {{simple_one_for_one, 0, 1},
          [{channel_sup, {rabbit_channel_sup, start_link, []},
            temporary, infinity, supervisor, [rabbit_channel_sup]}]}}.
