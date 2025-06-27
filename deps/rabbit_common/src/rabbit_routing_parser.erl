%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2013-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_routing_parser).

-export([parse_endpoint/1,
         parse_endpoint/2,
         parse_routing/1]).

parse_endpoint(Destination) ->
    parse_endpoint(Destination, false).

parse_endpoint(undefined, AllowAnonymousQueue) ->
    parse_endpoint("/queue", AllowAnonymousQueue);
parse_endpoint(Destination, AllowAnonymousQueue)
  when is_binary(Destination) ->
    List = unicode:characters_to_list(Destination),
    parse_endpoint(List, AllowAnonymousQueue);
parse_endpoint(Destination, AllowAnonymousQueue)
  when is_list(Destination) ->
    case re:split(Destination, "/", [unicode, {return, list}]) of
        [] -> %% in OTP28+, re:split("", "/") returns []
            {ok, {queue, unescape("")}};
        [Name] -> %% before OTP28, re:split("", "/") returns [[]]
            {ok, {queue, unescape(Name)}};
        ["", Type | Rest]
            when Type =:= "exchange" orelse Type =:= "queue" orelse
                 Type =:= "topic"    orelse Type =:= "temp-queue" ->
            parse_endpoint0(atomise(Type), Rest, AllowAnonymousQueue);
        ["", "amq", "queue" | Rest] ->
            parse_endpoint0(amqqueue, Rest, AllowAnonymousQueue);
        ["", "reply-queue" = Prefix | [_|_]] ->
            parse_endpoint0(reply_queue,
                            [lists:nthtail(2 + length(Prefix), Destination)],
                            AllowAnonymousQueue);
        _ ->
            {error, {unknown_destination, Destination}}
    end.

parse_endpoint0(exchange, ["" | _] = Rest,    _) ->
    {error, {invalid_destination, exchange, to_url(Rest)}};
parse_endpoint0(exchange, [Name],             _) ->
    {ok, {exchange, {unescape(Name), undefined}}};
parse_endpoint0(exchange, [Name, Pattern],    _) ->
    {ok, {exchange, {unescape(Name), unescape(Pattern)}}};
parse_endpoint0(queue,    [],                 false) ->
    {error, {invalid_destination, queue, []}};
parse_endpoint0(queue,    [],                 true) ->
    {ok, {queue, undefined}};
parse_endpoint0(Type,     [[_|_]] = [Name],   _) ->
    {ok, {Type, unescape(Name)}};
parse_endpoint0(Type,     Rest,               _) ->
    {error, {invalid_destination, Type, to_url(Rest)}}.

parse_routing({exchange, {Name, undefined}}) ->
    {Name, ""};
parse_routing({exchange, {Name, Pattern}}) ->
    {Name, Pattern};
parse_routing({topic, Name}) ->
    {"amq.topic", Name};
parse_routing({Type, Name})
  when Type =:= queue orelse Type =:= reply_queue orelse Type =:= amqqueue ->
    {"", Name}.

atomise(Name) when is_list(Name) ->
    list_to_atom(re:replace(Name, "-", "_", [{return,list}, global])).

to_url([])  -> [];
to_url(Lol) -> "/" ++ string:join(Lol, "/").

unescape(Str) -> unescape(Str, []).

unescape("%2F" ++ Str, Acc) -> unescape(Str, [$/ | Acc]);
unescape([C | Str],    Acc) -> unescape(Str, [C | Acc]);
unescape([],           Acc) -> lists:reverse(Acc).
