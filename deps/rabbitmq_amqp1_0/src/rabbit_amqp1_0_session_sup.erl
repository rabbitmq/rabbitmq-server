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
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_amqp1_0_session_sup).

-behaviour(supervisor2).

-export([start_link/1]).

-export([init/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([start_link_args/0]).

-type(start_link_args() ::
        {rabbit_types:protocol(), rabbit_net:socket(),
         rabbit_channel:channel_number(), non_neg_integer(), pid(),
         rabbit_access_control:username(), rabbit_types:vhost(), pid()}).

-spec(start_link/1 :: (start_link_args()) -> {'ok', pid(), pid()}).

-endif.

%%----------------------------------------------------------------------------
start_link({Protocol, Sock, Channel, FrameMax, ReaderPid, Username, VHost,
            Collector}) ->
    case Protocol of
        rabbit_amqp1_0_framing ->
            start_link_1_0({Protocol, Sock, Channel, FrameMax, ReaderPid,
                            Username, VHost, Collector});
        _ ->
            rabbit_channel_sup:start_link({Protocol, Sock, Channel, FrameMax,
                                           ReaderPid, Username, VHost,
                                           Collector})
    end.

start_link_1_0({Protocol, Sock, Channel, FrameMax, ReaderPid, Username, VHost,
                Collector}) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    {ok, WriterPid} =
        supervisor2:start_child(
          SupPid,
          {writer, {rabbit_amqp1_0_writer, start_link,
                    [Sock, Channel, FrameMax, Protocol, ReaderPid]},
           intrinsic, ?MAX_WAIT, worker, [rabbit_amqp1_0_writer]}),
    {ok, ChannelPid} =
        supervisor2:start_child(
          SupPid,
          {channel, {rabbit_amqp1_0_session_process, start_link,
                     [Channel, ReaderPid, WriterPid, Username, VHost,
                      Collector, start_limiter_fun(SupPid)]},
           intrinsic, ?MAX_WAIT, worker, [rabbit_amqp1_0_session_process]}),
    %% Not bothering with the framing channel just yet
    {ok, SupPid, ChannelPid}.

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
