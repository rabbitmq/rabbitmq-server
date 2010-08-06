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
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.
%%
%%   Copyright (C) 2009 LShift Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_management_queues).

-export([init/1, to_json/2, content_types_provided/2, is_authorized/2]).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(_Config) -> {ok, undefined}.
%%init(_Config) -> {{trace, "/tmp"}, undefined}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

to_json(ReqData, Context) ->
    Qs = rabbit_management_stats:get_queues(),
    {rabbit_management_format:encode(
       [{queues, [{struct, format(Q)} || Q <- Qs]}]), ReqData, Context}.

format(Q) ->
    MsgsReady = rabbit_management_stats:pget(messages_ready, Q),
    MsgsUnacked = rabbit_management_stats:pget(messages_unacknowledged, Q),
    rabbit_management_format:format(
      [{messages, rabbit_management_stats:add(MsgsReady, MsgsUnacked)}] ++ Q,
      [{fun rabbit_management_format:resource/1, [name]},
       {fun rabbit_management_format:pid/1,
        [pid, owner_pid, exclusive_consumer_pid]},
       {fun rabbit_management_format:table/1, [backing_queue_status]}]).

is_authorized(ReqData, Context) ->
    rabbit_management_util:is_authorized(ReqData, Context).
