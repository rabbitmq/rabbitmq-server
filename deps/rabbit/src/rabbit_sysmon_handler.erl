%% Copyright (c) 2011 Basho Technologies, Inc.  All Rights Reserved.
%% Copyright (c) 2018-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   https://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.

%% @doc A custom event handler to the `sysmon_handler' application's
%% `system_monitor' event manager.
%%
%% This module attempts to discover more information about a process
%% that generates a system_monitor event.

-module(rabbit_sysmon_handler).

-behaviour(gen_event).

%% API
-export([add_handler/0]).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state, {timer_ref :: reference() | undefined}).

-define(INACTIVITY_TIMEOUT, 5000).

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================

add_handler() ->
    %% Vulnerable to race conditions (installing handler multiple
    %% times), but risk is zero in the common OTP app startup case.
    case lists:member(?MODULE, gen_event:which_handlers(sysmon_handler)) of
        true ->
            ok;
        false ->
            sysmon_handler_filter:add_custom_handler(?MODULE, [])
    end.

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a new event handler is added to an event manager,
%% this function is called to initialize the event handler.
%%
%% @spec init(Args) -> {ok, State}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, #state{}, hibernate}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever an event manager receives an event sent using
%% gen_event:notify/2 or gen_event:sync_notify/2, this function is
%% called for each installed event handler to handle the event.
%%
%% @spec handle_event(Event, State) ->
%%                          {ok, State} |
%%                          {swap_handler, Args1, State1, Mod2, Args2} |
%%                          remove_handler
%% @end
%%--------------------------------------------------------------------
handle_event({monitor, Pid, Type, _Info},
             State=#state{timer_ref=TimerRef}) when Pid == self() ->
    %% Reset the inactivity timeout
    NewTimerRef = reset_timer(TimerRef),
    maybe_collect_garbage(Type),
    {ok, State#state{timer_ref=NewTimerRef}};
handle_event({monitor, PidOrPort, Type, Info}, State=#state{timer_ref=TimerRef}) ->
    %% Reset the inactivity timeout
    NewTimerRef = reset_timer(TimerRef),
    {Fmt, Args} = format_pretty_proc_or_port_info(PidOrPort),
    rabbit_log:warning("~p ~w ~w " ++ Fmt ++ " ~w", [?MODULE, Type, PidOrPort] ++ Args ++ [Info]),
    {ok, State#state{timer_ref=NewTimerRef}};
handle_event({suppressed, Type, Info}, State=#state{timer_ref=TimerRef}) ->
    %% Reset the inactivity timeout
    NewTimerRef = reset_timer(TimerRef),
    rabbit_log:debug("~p encountered a suppressed event of type ~w: ~w", [?MODULE, Type, Info]),
    {ok, State#state{timer_ref=NewTimerRef}};
handle_event(Event, State=#state{timer_ref=TimerRef}) ->
    NewTimerRef = reset_timer(TimerRef),
    rabbit_log:warning("~p unhandled event: ~p", [?MODULE, Event]),
    {ok, State#state{timer_ref=NewTimerRef}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever an event manager receives a request sent using
%% gen_event:call/3,4, this function is called for the specified
%% event handler to handle the request.
%%
%% @spec handle_call(Request, State) ->
%%                   {ok, Reply, State} |
%%                   {swap_handler, Reply, Args1, State1, Mod2, Args2} |
%%                   {remove_handler, Reply}
%% @end
%%--------------------------------------------------------------------
handle_call(_Call, State) ->
    Reply = not_supported,
    {ok, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called for each installed event handler when
%% an event manager receives any other message than an event or a
%% synchronous request (or a system message).
%%
%% @spec handle_info(Info, State) ->
%%                         {ok, State} |
%%                         {swap_handler, Args1, State1, Mod2, Args2} |
%%                         remove_handler
%% @end
%%--------------------------------------------------------------------
handle_info(inactivity_timeout, State) ->
    %% No events have arrived for the timeout period
    %% so hibernate to free up resources.
    {ok, State, hibernate};
handle_info(Info, State) ->
    rabbit_log:info("handle_info got ~p", [Info]),
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever an event handler is deleted from an event manager, this
%% function is called. It should be the opposite of Module:init/1 and
%% do any necessary cleaning up.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

format_pretty_proc_or_port_info(PidOrPort) ->
    try
        case get_pretty_proc_or_port_info(PidOrPort) of
            undefined ->
                {"", []};
            Res ->
                Res
        end
    catch C:E:S ->
        {"Pid ~w, ~W ~W at ~w\n",
            [PidOrPort, C, 20, E, 20, S]}
    end.

get_pretty_proc_or_port_info(Pid) when is_pid(Pid) ->
    Infos = [registered_name, initial_call, current_function, message_queue_len],
    case process_info(Pid, Infos) of
        undefined ->
            undefined;
        [] ->
            undefined;
        [{registered_name, RN0}, ICT1, {_, CF}, {_, MQL}] ->
            ICT = case proc_lib:translate_initial_call(Pid) of
                     {proc_lib, init_p, 5} ->   % not by proc_lib, see docs
                         ICT1;
                     ICT2 ->
                         {initial_call, ICT2}
                 end,
            RNL = if RN0 == [] -> [];
                     true      -> [{name, RN0}]
                  end,
            {"~w", [RNL ++ [ICT, CF, {message_queue_len, MQL}]]}
    end;
get_pretty_proc_or_port_info(Port) when is_port(Port) ->
    PortInfo = erlang:port_info(Port),
    {value, {name, Name}, PortInfo2} = lists:keytake(name, 1, PortInfo),
    QueueSize = [erlang:port_info(Port, queue_size)],
    Connected = case proplists:get_value(connected, PortInfo2) of
                    undefined ->
                        [];
                    ConnectedPid ->
                        case proc_lib:translate_initial_call(ConnectedPid) of
                            {proc_lib, init_p, 5} ->   % not by proc_lib, see docs
                                [];
                            ICT ->
                                [{initial_call, ICT}]
                        end
                end,
    {"name ~s ~w", [Name, lists:append([PortInfo2, QueueSize, Connected])]}.


%% @doc If the message type is due to a large heap warning
%% and the source is ourself, go ahead and collect garbage
%% to avoid the death spiral.
-spec maybe_collect_garbage(atom()) -> ok.
maybe_collect_garbage(large_heap) ->
    erlang:garbage_collect(),
    ok;
maybe_collect_garbage(_) ->
    ok.

-spec reset_timer(undefined | reference()) -> reference().
reset_timer(undefined) ->
    erlang:send_after(?INACTIVITY_TIMEOUT, self(), inactivity_timeout);
reset_timer(TimerRef) ->
    _ = erlang:cancel_timer(TimerRef),
    reset_timer(undefined).
