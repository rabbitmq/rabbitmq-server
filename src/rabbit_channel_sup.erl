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

-module(rabbit_channel_sup).

-behaviour(supervisor2).

-export([start_link/7, writer/1, framing_channel/1, channel/1]).

-export([init/1]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/7 ::
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), pid(), rabbit_access_control:username(),
         rabbit_types:vhost(), pid()) ->
                           ignore | rabbit_types:ok_or_error2(pid(), any())).

-endif.

%%----------------------------------------------------------------------------

start_link(Sock, Channel, FrameMax, ReaderPid, Username, VHost, Collector) ->
    supervisor2:start_link(?MODULE, [Sock, Channel, FrameMax, ReaderPid,
                                     Username, VHost, Collector]).

writer(Pid) ->
    hd(supervisor2:find_child(Pid, writer, worker, [rabbit_writer])).

channel(Pid) ->
    hd(supervisor2:find_child(Pid, channel, worker, [rabbit_channel])).

framing_channel(Pid) ->
    hd(supervisor2:find_child(Pid, framing_channel, worker,
                              [rabbit_framing_channel])).

%%----------------------------------------------------------------------------

init([Sock, Channel, FrameMax, ReaderPid, Username, VHost, Collector]) ->
    {ok, {{one_for_all, 0, 1},
          [{framing_channel, {rabbit_framing_channel, start_link, []},
            permanent, ?MAX_WAIT, worker, [rabbit_framing_channel]},
           {writer, {rabbit_writer, start_link, [Sock, Channel, FrameMax]},
            permanent, ?MAX_WAIT, worker, [rabbit_writer]},
           {channel, {rabbit_channel, start_link,
                      [Channel, ReaderPid, Username, VHost, Collector]},
            permanent, ?MAX_WAIT, worker, [rabbit_channel]}
          ]}}.
