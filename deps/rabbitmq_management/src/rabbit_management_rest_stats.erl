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
-module(rabbit_management_rest_stats).

-export([init/1, resource_exists/2, to_json/2, content_types_provided/2,
         is_authorized/2]).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(TYPES, [channel_exchange_stats, channel_queue_stats,
                channel_queue_exchange_stats]).

%%--------------------------------------------------------------------

init(_Config) -> {ok, undefined}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

type(ReqData) ->
    case dict:find(type, wrq:path_info(ReqData)) of
        error    -> error;
        {ok, Type} -> list_to_atom(Type)
    end.

resource_exists(ReqData, Context) ->
    case type(ReqData) of
        error -> {false, ReqData, Context};
        Type  -> {lists:member(Type, ?TYPES), ReqData, Context}
    end.

to_json(ReqData, Context) ->
    Type = type(ReqData),
    GroupBy = wrq:get_qs_value("group_by", ReqData),
    MatchKey = wrq:get_qs_value("match_key", ReqData),
    MatchValue = wrq:get_qs_value("match_value", ReqData),
    Stats = rabbit_management_stats:get_msg_stats(Type, GroupBy,
                                                  MatchKey, MatchValue),
    Res = {stats, [{struct, format(S)} || S <- Stats]},
    {rabbit_management_format:encode([{match_key_todo, MatchKey},
                                      {match_value_todo, MatchValue},
                                      Res
                                     ]), ReqData, Context}.

format({Ids, Stats}) ->
    [{stats, {struct, Stats}}|
     rabbit_management_format:format(
       Ids, [{fun rabbit_management_format:ip/1, [channel_peer_address]},
             {fun rabbit_management_format:pid/1, [channel_connection]}])].

is_authorized(ReqData, Context) ->
    rabbit_management_util:is_authorized(ReqData, Context).
