%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_boot_state_xterm_titlebar).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         terminate/2,
         code_change/3]).

-record(?MODULE, {raw_stdio_port}).

-define(LOG_PREFIX, "Boot state/xterm: ").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    State = case os:type() of
                {unix, _} ->
                    RawStdio = erlang:open_port({fd, 0, 1}, [out]),
                    #?MODULE{raw_stdio_port = RawStdio};
                _ ->
                    #?MODULE{}
            end,
    {ok, State}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({notify_boot_state, BootState}, State) ->
    _ = set_xterm_titlebar(State, BootState),
    {noreply, State}.

terminate(normal, #?MODULE{raw_stdio_port = RawStdio}) ->
    erlang:port_close(RawStdio),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Private

set_xterm_titlebar(#?MODULE{raw_stdio_port = RawStdio}, BootState) ->
    Title = format_title(BootState),
    Binary = unicode:characters_to_binary(Title),
    erlang:port_command(RawStdio, ["\033]2;", Binary, "\007"]).

format_title(BootState) ->
    %% FIXME: Move product info to prelaunch?
    Vsn = rabbit_misc:version(),
    BootStateSuffix = case BootState of
                          ready -> "";
                          _     -> io_lib:format(": ~ts", [BootState])
                      end,
    case node() of
        nonode@nohost ->
            rabbit_misc:format(
              "RabbitMQ ~ts~ts", [Vsn, BootStateSuffix]);
        Node ->
            rabbit_misc:format(
              "~s — RabbitMQ ~ts~ts", [Node, Vsn, BootStateSuffix])
    end.
