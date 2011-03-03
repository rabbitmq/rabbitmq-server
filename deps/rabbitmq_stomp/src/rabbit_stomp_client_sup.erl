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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_stomp_client_sup).
-behaviour(supervisor2).

-define(MAX_WAIT, 16#ffffffff).
-export([start_link/1, init/1]).

start_link(Sock) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    {ok, ProcessorPid} =
        supervisor2:start_child(SupPid,
                               {rabbit_stomp_processor,
                                {rabbit_stomp_processor, start_link,
                                 [Sock, rabbit_heartbeat:start_heartbeat_fun(SupPid)]},
                                intrinsic, ?MAX_WAIT, worker,
                                [rabbit_stomp_processor]}),
    {ok, ReaderPid} =
        supervisor2:start_child(SupPid,
                               {rabbit_stomp_reader,
                                {rabbit_stomp_reader,
                                 start_link, [ProcessorPid]},
                                intrinsic, ?MAX_WAIT, worker,
                                [rabbit_stomp_reader]}),
    {ok, SupPid, ReaderPid}.

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.


