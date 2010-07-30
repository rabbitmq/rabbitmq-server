%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is the RabbitMQ Erlang Client.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C)
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): Ben Hood <0x6e6562@gmail.com>.

%% @private
-module(amqp_channels_manager).

-include("amqp_client.hrl").

-behaviour(gen_server).

-export([start_link/1, register/4, new_number/2, is_empty/1,
         num_channels/1, get_pid/2, get_framing/2,
         signal_connection_closing/3]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).

-record(state, {sup,
                map_num_pid = gb_trees:empty(), %% Number -> {Pid, Framing}
                map_pid_num = dict:new(),       %% Pid -> Number
                max_channel,
                closing = false}).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link(MaxChannel) ->
    gen_server:start_link(?MODULE, [self(), MaxChannel], []).

%% FramingPid is 'undefined' in the direct case
register(Manager, ChannelNumber, ChannelPid, FramingPid) ->
    gen_server:call(Manager, {register, ChannelNumber, ChannelPid, FramingPid}).

new_number(Manager, ProposedNumber) ->
    gen_server:call(Manager, {new_number, ProposedNumber}, infinity).

is_empty(Manager) ->
    gen_server:call(Manager, is_empty, infinity).

num_channels(Manager) ->
    gen_server:call(Manager, num_channels, infinity).

get_pid(Manager, ChannelNumber) ->
    gen_server:call(Manager, {get_pid, ChannelNumber}, infinity).

get_framing(Manager, ChannelNumber) ->
    gen_server:call(Manager, {get_framing, ChannelNumber}, infinity).

signal_connection_closing(Manager, ChannelCloseType, Reason) ->
    gen_server:cast(Manager, {connection_closing, ChannelCloseType, Reason}).

%%---------------------------------------------------------------------------
%% gen_server callbacks
%%---------------------------------------------------------------------------

init([Sup, MaxChannel]) ->
    MaxChannel2 = if MaxChannel =:= 0 -> ?MAX_CHANNEL_NUMBER;
                     true             -> MaxChannel
                  end,
    {ok, #state{sup = Sup, max_channel = MaxChannel2}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.

handle_call({register, Number, Pid, Framing}, _From,
            State = #state{closing = false}) ->
    handle_register(Number, Pid, Framing, State);
handle_call({new_number, Proposed}, _From, State = #state{closing = false}) ->
    handle_new_number(Proposed, State);
handle_call(is_empty, _From, State) ->
    {reply, internal_is_empty(State), State};
handle_call(num_channels, _From, State) ->
    {reply, internal_num_channels(State), State};
handle_call({get_pid, Number}, _, State = #state{map_num_pid = MapNP}) ->
    {reply, case gb_trees:lookup(Number, MapNP) of
                {value, {Pid, _}} -> Pid;
                none              -> undefined
            end, State};
handle_call({get_framing, Number}, _, State = #state{map_num_pid = MapNP}) ->
    {reply, case gb_trees:lookup(Number, MapNP) of
                {value, {_, Framing}} -> Framing;
                none                  -> undefined
            end, State}.

handle_cast({connection_closing, ChannelCloseType, Reason}, State) ->
    handle_connection_closing(ChannelCloseType, Reason, State).

handle_info({'DOWN', _, process, Pid, Reason}, State) ->
    handle_down(Pid, Reason, State).

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

handle_new_number(none, State = #state{max_channel = MaxChannel,
                                       map_num_pid = MapNP}) ->
    Reply =
        case gb_trees:is_empty(MapNP) of
            true  -> {ok, 1};
            false -> {Smallest, _} = gb_trees:smallest(MapNP),
                     if Smallest > 1 ->
                            {ok, Smallest - 1};
                        true ->
                            {Largest, _} = gb_trees:largest(MapNP),
                            if Largest < MaxChannel -> {ok, Largest + 1};
                               true                 -> find_free(MapNP)
                            end
                     end
        end,
    {reply, Reply, State};
handle_new_number(Proposed, State = #state{max_channel = MaxChannel,
                                           map_num_pid = MapNP}) ->
    IsValid = Proposed > 0 andalso Proposed =< MaxChannel andalso
        not gb_trees:is_defined(Proposed, MapNP),
    if IsValid -> {reply, {ok, Proposed}, State};
       true    -> handle_new_number(none, State)
    end.

find_free(MapNP) ->
    find_free(gb_trees:iterator(MapNP), 1).

find_free(It, Candidate) ->
    case gb_trees:next(It) of
        {Number, _, It1} -> if Number > Candidate   -> {ok, Number - 1};
                               Number =:= Candidate -> find_free(It1,
                                                                 Candidate + 1);
                               true                 -> exit(unexpected)
                            end;
        none             -> {error, out_of_channel_numbers}
    end.

handle_register(Number, Pid, Framing, State = #state{map_num_pid = MapNP,
                                                     map_pid_num = MapPN}) ->
    IsValid = not gb_trees:is_defined(Number, MapNP) andalso
                  not dict:is_key(Pid, MapPN),
    if IsValid -> Ref = erlang:monitor(process, Pid),
                  receive {'DOWN', Ref, process, Pid, noproc} ->
                      {reply, {error, noproc}, State}
                  after 0 ->
                      MapNP1 = gb_trees:enter(Number, {Pid, Framing}, MapNP),
                      MapPN1 = dict:store(Pid, Number, MapPN),
                      {reply, ok, State#state{map_num_pid = MapNP1,
                                              map_pid_num = MapPN1}}
                  end;
       true    -> {stop, {error, {already_exists, Number, Pid, Framing}}, State}
    end.

handle_down(Pid, Reason, State = #state{map_pid_num = MapPN}) ->
    case dict:fetch(Pid, MapPN) of
        undefined -> {stop, {error, unexpected_down}, State};
        Number    -> handle_channel_down(Pid, Number, Reason, State)
    end.

handle_channel_down(Pid, Number, Reason, State) ->
    down_side_effect(Pid, Reason, State),
    NewState = internal_unregister(Pid, Number, State),
    check_all_channels_terminated(NewState),
    {noreply, NewState}.

down_side_effect(_Pid, normal, _State) ->
    ok;
down_side_effect(Pid, {server_initiated_close, Code, _Text} = Reason, State) ->
    {IsHardError, _, _} = rabbit_framing:lookup_amqp_exception(
                            rabbit_framing:amqp_exception(Code)),
    case IsHardError of
        true  -> signal_connection({hard_error_in_channel, Pid, Reason}, State);
        false -> ok
    end;
down_side_effect(_Pid, {app_initiated_close, _, _}, _State) ->
    ok;
down_side_effect(_Pid, {connection_closing, _}, _State) ->
    ok;
down_side_effect(Pid, Other, State) ->
    signal_connection({channel_internal_error, Pid, Other}, State).

check_all_channels_terminated(#state{closing = false}) ->
    ok;
check_all_channels_terminated(State = #state{closing = true}) ->
    case internal_is_empty(State) of
        true  -> signal_connection(all_channels_terminated, State);
        false -> ok
    end.

handle_connection_closing(ChannelCloseType, Reason, State) ->
    case internal_is_empty(State) of
        true  -> signal_connection(all_channels_terminated, State);
        false -> signal_channels({connection_closing, ChannelCloseType, Reason},
                                 State)
    end,
    {noreply, State#state{closing = true}}.

internal_unregister(Pid, Number, State = #state{map_num_pid = MapNP,
                                                map_pid_num = MapPN}) ->
    MapNP1 = gb_trees:delete(Number, MapNP),
    MapPN1 = dict:erase(Pid, MapPN),
    State#state{map_num_pid = MapNP1, map_pid_num = MapPN1}.

internal_is_empty(#state{map_num_pid = MapNP}) ->
    gb_trees:is_empty(MapNP).

internal_num_channels(#state{map_num_pid = MapNP}) ->
    gb_trees:size(MapNP).

signal_channels(Msg, #state{map_pid_num = MapPN}) ->
    dict:map(fun(Pid, _) -> Pid ! Msg, ok end, MapPN),
    ok.

signal_connection(Msg, #state{sup = Sup}) ->
    amqp_infra_sup:child(Sup, connection) ! Msg,
    ok.
