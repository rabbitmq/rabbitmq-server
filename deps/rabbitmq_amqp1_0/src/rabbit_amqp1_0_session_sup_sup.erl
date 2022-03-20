%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqp1_0_session_sup_sup).

-behaviour(supervisor2).

-export([start_link/0, start_session/2]).

-export([init/1]).

%% It would be much nicer if rabbit_channel_sup_sup was parameterised
%% on the module.

%%----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().
-spec start_session(pid(), rabbit_amqp1_0_session_sup:start_link_args()) ->
                              {'ok', pid(), pid()}.

%%----------------------------------------------------------------------------

start_link() ->
    supervisor2:start_link(?MODULE, []).

start_session(Pid, Args) ->
    supervisor2:start_child(Pid, [Args]).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{session_sup, {rabbit_amqp1_0_session_sup, start_link, []},
            temporary, infinity, supervisor, [rabbit_amqp1_0_session_sup]}]}}.
