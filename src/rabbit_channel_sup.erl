%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_channel_sup).

-behaviour(supervisor2).

-export([start_link/1]).

-export([init/1]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([start_link_args/0]).

-type(start_link_args() ::
        {'tcp', rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), pid(), rabbit_types:protocol(), rabbit_types:user(),
         rabbit_types:vhost(), rabbit_framing:amqp_table(),
         pid()} |
        {'direct', rabbit_channel:channel_number(), pid(),
         rabbit_types:protocol(), rabbit_types:user(), rabbit_types:vhost(),
         rabbit_framing:amqp_table(), pid()}).

-spec(start_link/1 :: (start_link_args()) -> {'ok', pid(), {pid(), any()}}).

-endif.

%%----------------------------------------------------------------------------

start_link({tcp, Sock, Channel, FrameMax, ReaderPid, Module, Protocol,
            User, VHost, Capabilities, Collector}) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    {ok, WriterPid} =
        supervisor2:start_child(
          SupPid,
          {writer, {rabbit_writer, start_link,
                    [Sock, Channel, FrameMax, Module, Protocol, ReaderPid]},
           intrinsic, ?MAX_WAIT, worker, [rabbit_writer]}),
    {ok, ChannelPid} =
        supervisor2:start_child(
          SupPid, Module:channel_spec(
                    [Channel, ReaderPid, WriterPid, ReaderPid, Protocol,
                     User, VHost, Capabilities, Collector,
                     start_limiter_fun(SupPid)])),
    {ok, AState} = rabbit_command_assembler:init(Protocol),
    {ok, SupPid, {ChannelPid, AState}};
start_link({direct, Channel, ClientChannelPid, ConnPid, Protocol, User, VHost,
            Capabilities, Collector}) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    {ok, ChannelPid} =
        supervisor2:start_child(
          SupPid,
          {channel, {rabbit_channel, start_link,
                     [Channel, ClientChannelPid, ClientChannelPid, ConnPid,
                      Protocol, User, VHost, Capabilities, Collector,
                      start_limiter_fun(SupPid)]},
           intrinsic, ?MAX_WAIT, worker, [rabbit_channel]}),
    {ok, SupPid, {ChannelPid, none}}.

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.

start_limiter_fun(SupPid) ->
    fun (UnackedCount) ->
            Me = self(),
            {ok, _Pid} =
                supervisor2:start_child(
                  SupPid,
                  {limiter, {rabbit_limiter, start_link, [Me, UnackedCount]},
                   transient, ?MAX_WAIT, worker, [rabbit_limiter]})
    end.
