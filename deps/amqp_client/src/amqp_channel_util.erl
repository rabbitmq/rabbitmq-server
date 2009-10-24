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
-module(amqp_channel_util).

-include("amqp_client.hrl").

-export([open_channel/4]).
-export([start_channel_infrastructure/3, terminate_channel_infrastructure/2]).
-export([do/4]).
-export([new_channel_dict/0, is_channel_dict_empty/1, register_channel/3,
         unregister_channel/2, resolve_channel/2, is_channel_registered/2,
         get_max_channel_number/1]).
-export([broadcast_to_channels/2]).

%%---------------------------------------------------------------------------
%% Opening channels
%%---------------------------------------------------------------------------

%% Spawns a new channel process linked to the calling process and registers it
%% in the given Channels dict
open_channel(ProposedNumber, Driver, StartArgs, Channels) ->
    ChannelNumber = assign_channel_number(ProposedNumber, Channels),
    {ok, ChannelPid} = gen_server:start_link(
        amqp_channel, {self(), ChannelNumber, Driver, StartArgs}, []),
    #'channel.open_ok'{} = amqp_channel:call(ChannelPid, #'channel.open'{}),
    NewChannels = register_channel(ChannelNumber, ChannelPid, Channels),
    {ChannelPid, NewChannels}.

assign_channel_number(none, Channels) ->
    %% TODO Implement support for channel_max from 'connection.tune'
    %% TODO Make it possible for channel numbers to be reused properly
    get_max_channel_number(Channels) + 1;
assign_channel_number(ChannelNumber, Channels) ->
    case is_channel_registered({channel, ChannelNumber}, Channels) of
        true  -> assign_channel_number(none, Channels);
        false -> ChannelNumber
    end.

%%---------------------------------------------------------------------------
%% Starting and terminating channel infrastructure
%%---------------------------------------------------------------------------

start_channel_infrastructure(network, ChannelNumber, {Sock, MainReader}) ->
    FramingPid = rabbit_framing_channel:start_link(fun(X) -> X end, [self()]),
    WriterPid = rabbit_writer:start_link(Sock, ChannelNumber, ?FRAME_MIN_SIZE),
    case MainReader of
        none ->
            ok;
        _ ->
            MainReader ! {register_framing_channel, ChannelNumber, FramingPid,
                          self()},
            MonitorRef = erlang:monitor(process, MainReader),
            receive
                registered_framing_channel ->
                    erlang:demonitor(MonitorRef), ok;
                {'DOWN', MonitorRef, process, MainReader, _Info} ->
                    erlang:error(main_reader_died_while_registering_framing)
            end
    end,
    {FramingPid, WriterPid};
start_channel_infrastructure(
        direct, ChannelNumber, #amqp_params{username = User,
                                            virtual_host = VHost}) ->
    Peer = rabbit_channel:start_link(ChannelNumber, self(), self(), User, VHost),
    {Peer, Peer}.

terminate_channel_infrastructure(network, {FramingPid, WriterPid}) ->
    rabbit_framing_channel:shutdown(FramingPid),
    rabbit_writer:shutdown(WriterPid),
    ok;
terminate_channel_infrastructure(direct, {Peer, Peer})->
    gen_server2:cast(Peer, terminate),
    ok.

%%---------------------------------------------------------------------------
%% Do
%%---------------------------------------------------------------------------

do(network, Writer, Method, Content) ->
    case Content of
        none -> rabbit_writer:send_command_and_signal_back(Writer, Method,
                                                           self());
        _    -> rabbit_writer:send_command_and_signal_back(Writer, Method,
                                                           Content, self())
    end,
    receive_writer_send_command_signal(Writer);
do(direct, Writer, Method, Content) ->
    case Content of
        none -> rabbit_channel:do(Writer, Method);
        _    -> rabbit_channel:do(Writer, Method, Content)
    end.

receive_writer_send_command_signal(Writer) ->
    receive
        rabbit_writer_send_command_signal   -> ok;
        WriterExitMsg = {'EXIT', Writer, _} -> self() ! WriterExitMsg
    end.

%%---------------------------------------------------------------------------
%% Channel number/pid registration
%%---------------------------------------------------------------------------

%% New channel dictionary for keeping track of the mapping between the channel
%% pid's and the channel numbers (the dictionary will essentially be used as a
%% bimap)
new_channel_dict() ->
    dict:new().

%% Returns true iff there are no channels currently registered in the given
%% dictionary
is_channel_dict_empty(Dict) ->
    dict:size(Dict) =:= 0.

%% Register a channel in a given channel dictionary
register_channel(Number, Pid, Dict) ->
    case dict:is_key({channel, Number}, Dict) of
        true ->
            erlang:error({channel_already_registered, Number});
        false ->
            Dict1 = dict:store({channel, Number}, {chpid, Pid}, Dict),
            dict:store({chpid, Pid}, {channel, Number}, Dict1)
    end.

%% Unregister a channel by passing either {channel, Number} or {chpid, Pid} for
%% Channel
unregister_channel(Channel, Dict) ->
    case dict:fetch(Channel, Dict) of
        undefined -> erlang:error(undefined);
        Val       -> dict:erase(Val, dict:erase(Channel, Dict))
    end.

%% Resolve channel by passing either {channel, Number} or {chpid, Pid} for
%% Channel
resolve_channel(Channel, Dict) ->
    dict:fetch(Channel, Dict).

%% Returns true iff Channel is registered in the given channel dictionary.
%% Pass either {channel, Number} or {chpid, Pid} for Channel
is_channel_registered(Channel, Dict) ->
    dict:is_key(Channel, Dict).

%% Returns the greatest channel number of the currently registered channels in
%% the given dictionary. Returns 0 if there are no channels registered.
get_max_channel_number(Dict) ->
    dict:fold(fun({channel, N}, _,  Max) when Max >= N -> Max;
                 ({channel, N}, _, _Max)               -> N;
                 ({chpid,   _}, _,  Max)               -> Max
              end, 0, Dict).

%%---------------------------------------------------------------------------
%% Other channel utilities
%%---------------------------------------------------------------------------

broadcast_to_channels(Message, Dict) ->
    dict:map(fun({chpid, Channel}, _) -> Channel ! Message, ok;
                ({channel, _}, _)     -> ok
             end, Dict),
    ok.
