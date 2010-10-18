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

-module(rabbit_stomp_util).

-export([parse_destination/1, parse_routing_information/1,
         create_message_id/3, parse_message_id/1]).

-define(QUEUE_PREFIX, "/queue").
-define(TOPIC_PREFIX, "/topic").
-define(EXCHANGE_PREFIX, "/exchange").

-define(MESSAGE_ID_SEPARATOR, "@@").

create_message_id(ConsumerTag, SessionId, DeliveryTag) ->
    [ConsumerTag,
     ?MESSAGE_ID_SEPARATOR,
     SessionId,
     ?MESSAGE_ID_SEPARATOR,
     integer_to_list(DeliveryTag)].

parse_message_id(MessageId) ->
    Pieces = re:split(MessageId, ?MESSAGE_ID_SEPARATOR, [{return, list}]),
    case Pieces of
        [ConsumerTag, SessionId, DeliveryTag] ->
            {ok, {list_to_binary(ConsumerTag),
                  SessionId,
                  list_to_integer(DeliveryTag)}};
        _ ->
            {error, invalid_message_id}
    end.

parse_destination(?QUEUE_PREFIX ++ Rest) ->
    parse_simple_destination(queue, Rest);
parse_destination(?TOPIC_PREFIX ++ Rest) ->
    parse_simple_destination(topic, Rest);
parse_destination(?EXCHANGE_PREFIX ++ Rest) ->
    case parse_content(Rest) of
        {ok, [Name]} -> {ok, {exchange, {Name, undefined}}};
        {ok, [Name, Pattern]} -> {ok, {exchange, {Name, Pattern}}};
        _ -> {error, {invalid_destination, exchange, Rest}}
    end;
parse_destination(Destination) ->
    {error, {unknown_destination, Destination}}.

parse_routing_information({exchange, {Name, undefined}}) ->
    {Name, ""};
parse_routing_information({exchange, {Name, Pattern}}) ->
    {Name, Pattern};
parse_routing_information({queue, Name}) ->
    {"", Name};
parse_routing_information({topic, Name}) ->
    {"amq.topic", Name}.

%% ---- Destination parsing helpers ----

parse_simple_destination(Type, Content) ->
    io:format("~p~n", [parse_content(Content)]),
    case parse_content(Content) of
        {ok, [Name]} -> {ok, {Type, Name}};
        _      -> {error, {invalid_destination, Type, Content}}
    end.

parse_content(Content)->
    case regexp:split(Content, "/") of
        {ok, Matches} -> {ok, strip_leading_blank_matches(Matches)};
        Other -> Other
    end.

strip_leading_blank_matches([[] | Rest]) ->
    %% We might get a few leading blank matches in cases such as
    %% /queue or /queue/. We don't want these in the result set.
    strip_leading_blank_matches(Rest);
strip_leading_blank_matches(Matches) ->
    Matches.

