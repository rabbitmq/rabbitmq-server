%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2015-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_boot_state_systemd).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         terminate/2,
         code_change/3]).

-record(state, {mechanism,
                sd_notify_module,
                socket}).

-define(LOG_PREFIX, "Boot state/systemd: ").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    case os:type() of
        {unix, _} ->
            case code:load_file(sd_notify) of
                {module, sd_notify} ->
                    {ok, #state{mechanism = legacy,
                                sd_notify_module = sd_notify}};
                {error, _} ->
                    case os:getenv("NOTIFY_SOCKET") of
                        false ->
                            ignore;
                        "" ->
                            ignore;
                        Socket ->
                            {ok, #state{mechanism = socat,
                                        socket = Socket}}
                    end
            end;
        _ ->
            ignore
    end.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({notify_boot_state, BootState}, State) ->
    notify_boot_state(BootState, State),
    {noreply, State}.

terminate(normal, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Private

notify_boot_state(ready = BootState,
                  #state{mechanism = legacy, sd_notify_module = SDNotify}) ->
    ?LOG_DEBUG(
      ?LOG_PREFIX "notifying of state `~s` (via native module)",
      [BootState],
      #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    sd_notify_legacy(SDNotify);
notify_boot_state(ready = BootState,
                  #state{mechanism = socat, socket = Socket}) ->
    ?LOG_DEBUG(
      ?LOG_PREFIX "notifying of state `~s` (via socat(1))",
      [BootState],
      #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    sd_notify_socat(Socket);
notify_boot_state(BootState, _) ->
    ?LOG_DEBUG(
      ?LOG_PREFIX "ignoring state `~s`",
      [BootState],
      #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ok.

sd_notify_message() ->
    "READY=1\nSTATUS=Initialized\nMAINPID=" ++ os:getpid() ++ "\n".

sd_notify_legacy(SDNotify) ->
    SDNotify:sd_notify(0, sd_notify_message()).

%% socat(1) is the most portable way the sd_notify could be
%% implemented in erlang, without introducing some NIF. Currently the
%% following issues prevent us from implementing it in a more
%% reasonable way:
%% - systemd-notify(1) is unstable for non-root users
%% - erlang doesn't support unix domain sockets.
%%
%% Some details on how we ended with such a solution:
%%   https://github.com/rabbitmq/rabbitmq-server/issues/664
sd_notify_socat(Socket) ->
    case sd_current_unit() of
        {ok, Unit} ->
            ?LOG_DEBUG(
              ?LOG_PREFIX "systemd unit for activation check: \"~s\"~n",
              [Unit],
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            sd_notify_socat(Socket, Unit);
        _ ->
            ok
    end.

sd_notify_socat(Socket, Unit) ->
    try sd_open_port(Socket) of
        Port ->
            Port ! {self(), {command, sd_notify_message()}},
            Result = sd_wait_activation(Port, Unit),
            port_close(Port),
            Result
    catch
        Class:Reason ->
            ?LOG_DEBUG(
              ?LOG_PREFIX "Failed to start socat(1): ~p:~p~n",
              [Class, Reason],
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            false
    end.

sd_current_unit() ->
    CmdOut = os:cmd("ps -o unit= -p " ++ os:getpid()),
    Ret = (catch re:run(CmdOut,
                        "([-.@0-9a-zA-Z]+)",
                        [unicode, {capture, all_but_first, list}])),
    case Ret of
        {'EXIT', _}     -> error;
        {match, [Unit]} -> {ok, Unit};
        _               -> error
    end.

socat_socket_arg("@" ++ AbstractUnixSocket) ->
    "abstract-sendto:" ++ AbstractUnixSocket;
socat_socket_arg(UnixSocket) ->
    "unix-sendto:" ++ UnixSocket.

sd_open_port(Socket) ->
    open_port(
        {spawn_executable, os:find_executable("socat")},
        [{args, [socat_socket_arg(Socket), "STDIO"]},
            use_stdio, out]).

sd_wait_activation(Port, Unit) ->
    case os:find_executable("systemctl") of
        false ->
            ?LOG_DEBUG(
              ?LOG_PREFIX "systemctl(1) unavailable, falling back to sleep~n",
              [],
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            timer:sleep(5000),
            ok;
        _ ->
            sd_wait_activation(Port, Unit, 10)
    end.

sd_wait_activation(_, _, 0) ->
    ?LOG_DEBUG(
      ?LOG_PREFIX "service still in 'activating' state, bailing out~n",
      [],
      #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ok;
sd_wait_activation(Port, Unit, AttemptsLeft) ->
    Ret = os:cmd("systemctl show --property=ActiveState -- '" ++ Unit ++ "'"),
    case Ret of
        "ActiveState=activating\n" ->
            timer:sleep(1000),
            sd_wait_activation(Port, Unit, AttemptsLeft - 1);
        "ActiveState=" ++ _ ->
            ok;
        _ = Err ->
            ?LOG_DEBUG(
              ?LOG_PREFIX "unexpected status from systemd: ~p~n", [Err],
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            ok
    end.
