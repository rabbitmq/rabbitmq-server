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

-module(rabbit_stomp_destination_parser).

-export([parse_destination/1]).

-define(QUEUE_PREFIX, "/queue").
-define(TOPIC_PREFIX, "/topic").
-define(EXCHANGE_PREFIX, "/exchange").

parse_destination(?QUEUE_PREFIX ++ Rest) ->
    parse_simple_destination(queue, Rest);
parse_destination(?TOPIC_PREFIX ++ Rest) ->
    parse_simple_destination(topic, Rest);
parse_destination(?EXCHANGE_PREFIX ++ Rest) ->
    case parse_content(Rest) of
        [Name] -> {ok, {exchange, {Name, undefined}}};
        [Name, Pattern] -> {ok, {exchange, {Name, Pattern}}};
        _ -> {error, {invalid_destination, exchange, Rest}}
    end;
parse_destination(Destination) ->
    {error, {unknown_destination, Destination}}.

parse_simple_destination(Type, Content) ->
    case parse_content(Content) of
        [Name] -> {ok, {Type, Name}};
        _      -> {error, {invalid_destination, Type, Content}}
    end.

parse_content(Content)->
    parse_content(Content, {[], []}).

parse_content([], State) ->
    lists:reverse(accumulate_part(State));
parse_content("/" ++ Rest, State) ->
    parse_content(Rest, {accumulate_part(State), []});
parse_content([C | Rest], {Parts, Acc}) ->
    parse_content(Rest, {Parts, [C | Acc]}).

accumulate_part({Parts, Acc}) ->
    case Acc of
        [] -> Parts;
        _ -> [lists:reverse(Acc) | Parts]
    end.



