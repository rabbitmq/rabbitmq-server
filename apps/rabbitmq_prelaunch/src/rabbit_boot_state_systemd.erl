%%%-------------------------------------------------------------------
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
%% Copyright (c) 2015-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_boot_state_systemd).

-behaviour(gen_server).

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
    rabbit_log_prelaunch:debug(
      ?LOG_PREFIX "notifying of state `~s` (via native module)",
      [BootState]),
    sd_notify_legacy(SDNotify);
notify_boot_state(ready = BootState,
                  #state{mechanism = socat, socket = Socket}) ->
    rabbit_log_prelaunch:debug(
      ?LOG_PREFIX "notifying of state `~s` (via socat(1))",
      [BootState]),
    sd_notify_socat(Socket);
notify_boot_state(BootState, _) ->
    rabbit_log_prelaunch:debug(
      ?LOG_PREFIX "ignoring state `~s`",
      [BootState]),
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
            rabbit_log_prelaunch:debug(
              ?LOG_PREFIX "systemd unit for activation check: \"~s\"~n",
              [Unit]),
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
            rabbit_log_prelaunch:debug(
              ?LOG_PREFIX "Failed to start socat(1): ~p:~p~n",
              [Class, Reason]),
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
            rabbit_log_prelaunch:debug(
              ?LOG_PREFIX "systemctl(1) unavailable, falling back to sleep~n"),
            timer:sleep(5000),
            ok;
        _ ->
            sd_wait_activation(Port, Unit, 10)
    end.

sd_wait_activation(_, _, 0) ->
    rabbit_log_prelaunch:debug(
      ?LOG_PREFIX "service still in 'activating' state, bailing out~n"),
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
            rabbit_log_prelaunch:debug(
              ?LOG_PREFIX "unexpected status from systemd: ~p~n", [Err]),
            ok
    end.
