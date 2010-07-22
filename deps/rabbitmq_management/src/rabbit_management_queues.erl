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
    Qs = mnesia:dirty_match_object(rabbit_queue, #amqqueue{_ = '_'}),
    QStats = lists:zip(Qs,rabbit_management_stats:get_queue_stats(
                            [Q#amqqueue.pid || Q <- Qs])),
    QInfos = [format(Q, Stats) || {Q, Stats} <- QStats],
    {mochijson2:encode({struct,
                        [{node, node()},
                         {pid, list_to_binary(os:getpid())},
                         {datetime, list_to_binary(
                                      rabbit_management_util:http_date())},
                         {queues, [{struct, I} || I <- QInfos]}
                        ]}), ReqData, Context}.

format(#amqqueue{name = Name,
                 durable = Durable,
                 auto_delete = AutoDelete,
                 exclusive_owner = ExclusiveOwner,
                 arguments = Arguments,
                 pid = QPid}, Stats) ->
    MsgsReady = rabbit_management_stats:pget(messages_ready, Stats),
    MsgsUnacked = rabbit_management_stats:pget(messages_unacknowledged, Stats),
    [{vhost, Name#resource.virtual_host},
     {name, Name#resource.name},
     {durable, Durable},
     {auto_delete, AutoDelete},
     {exclusive_owner, status_render:format_pid(ExclusiveOwner)},
     {arguments, Arguments},
     {pid, status_render:format_pid(QPid)},
     {exclusive_consumer_pid,
      status_render:format_pid(
        rabbit_management_stats:pget(exclusive_consumer_pid, Stats))},
     {exclusive_consumer_tag,
      rabbit_management_stats:pget(exclusive_consumer_tag, Stats)},
     {messages_ready, MsgsReady},
     {messages_unacknowledged, MsgsUnacked},
     {messages, rabbit_management_stats:add(MsgsReady, MsgsUnacked)},
     {consumers, rabbit_management_stats:pget(consumers, Stats)},
     {memory, rabbit_management_stats:pget(memory, Stats)},
     {backing_queue_status,
      rabbit_management_stats:pget(backing_queue_status, Stats)}
    ].

is_authorized(ReqData, Context) ->
    rabbit_management_util:is_authorized(ReqData, Context).
