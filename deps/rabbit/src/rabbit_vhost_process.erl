%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% This module implements a vhost identity process.

%% On start this process will try to recover the vhost data and
%% processes structure (queues and message stores).
%% If recovered successfully, the process will save it's PID
%% to vhost process registry. If vhost process PID is in the registry and the
%% process is alive - the vhost is considered running.

%% On termination, the ptocess will notify of vhost going down.

%% The process will also check periodically if the vhost still
%% present in mnesia DB and stop the vhost supervision tree when it
%% disappears.

-module(rabbit_vhost_process).

%% Transitional step until we can require Erlang/OTP 21 and
%% use the now recommended try/catch syntax for obtaining the stack trace.
-compile(nowarn_deprecated_function).

-include_lib("rabbit_common/include/rabbit.hrl").

-define(TICKTIME_RATIO, 4).

-behaviour(gen_server2).
-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

start_link(VHost) ->
    gen_server2:start_link(?MODULE, [VHost], []).


init([VHost]) ->
    process_flag(trap_exit, true),
    rabbit_log:debug("Recovering data for VHost ~p", [VHost]),
    try
        %% Recover the vhost data and save it to vhost registry.
        ok = rabbit_vhost:recover(VHost),
        rabbit_vhost_sup_sup:save_vhost_process(VHost, self()),
        Interval = interval(),
        timer:send_interval(Interval, check_vhost),
        true = erlang:garbage_collect(),
        {ok, VHost}
    catch _:Reason:Stacktrace ->
        rabbit_amqqueue:mark_local_durable_queues_stopped(VHost),
        rabbit_log:error("Unable to recover vhost ~p data. Reason ~p~n"
                         " Stacktrace ~p",
                         [VHost, Reason, Stacktrace]),
        {stop, Reason}
    end.

handle_call(_,_,VHost) ->
    {reply, ok, VHost}.

handle_cast(_, VHost) ->
    {noreply, VHost}.

handle_info(check_vhost, VHost) ->
    case rabbit_vhost:exists(VHost) of
        true  -> {noreply, VHost};
        false ->
            rabbit_log:warning("Virtual host '~s' is gone. "
                               "Stopping its top level supervisor.",
                               [VHost]),
            %% Stop vhost's top supervisor in a one-off process to avoid a deadlock:
            %% us (a child process) waiting for supervisor shutdown and our supervisor(s)
            %% waiting for us to shutdown.
            spawn(
                fun() ->
                    rabbit_vhost_sup_sup:stop_and_delete_vhost(VHost)
                end),
            {noreply, VHost}
    end;
handle_info(_, VHost) ->
    {noreply, VHost}.

terminate(shutdown, VHost) ->
    %% Notify that vhost is stopped.
    rabbit_vhost:vhost_down(VHost);
terminate(_, _VHost) ->
    ok.

code_change(_OldVsn, VHost, _Extra) ->
    {ok, VHost}.

interval() ->
    application:get_env(kernel, net_ticktime, 60000) * ?TICKTIME_RATIO.
