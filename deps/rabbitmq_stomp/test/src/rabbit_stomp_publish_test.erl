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
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2011 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_stomp_publish_test).

-export([run/0]).

-include("rabbit_stomp_frame.hrl").

-define(DESTINATION, "/queue/test").

-define(MICROS_PER_UPDATE,     5000000).
-define(MICROS_PER_UPDATE_MSG, 100000).
-define(MICROS_PER_SECOND,     1000000).

%% A very simple publish-and-consume-as-fast-as-you-can test.

run() ->
    [put(K, 0) || K <- [sent, recd, last_sent, last_recd]],
    put(last_ts, erlang:monotonic_time()),
    {ok, Pub} = rabbit_stomp_client:connect(),
    {ok, Recv} = rabbit_stomp_client:connect(),
    Self = self(),
    spawn(fun() -> publish(Self, Pub, 0, erlang:monotonic_time()) end),
    rabbit_stomp_client:send(
      Recv, "SUBSCRIBE", [{"destination", ?DESTINATION}]),
    spawn(fun() -> recv(Self, Recv, 0, erlang:monotonic_time()) end),
    report().

report() ->
    receive
        {sent, C} -> put(sent, C);
        {recd, C} -> put(recd, C)
    end,
    Diff = erlang:convert_time_unit(
      erlang:monotonic_time() - get(last_ts), native, microseconds),
    case Diff > ?MICROS_PER_UPDATE of
        true  -> S = get(sent) - get(last_sent),
                 R = get(recd) - get(last_recd),
                 put(last_sent, get(sent)),
                 put(last_recd, get(recd)),
                 put(last_ts, erlang:monotonic_time()),
                 io:format("Send ~p msg/s | Recv ~p msg/s~n",
                           [trunc(S * ?MICROS_PER_SECOND / Diff),
                            trunc(R * ?MICROS_PER_SECOND / Diff)]);
        false -> ok
    end,
    report().

publish(Owner, Client, Count, TS) ->
    rabbit_stomp_client:send(
      Client, "SEND", [{"destination", ?DESTINATION}],
      [integer_to_list(Count)]),
    Diff = erlang:convert_time_unit(
      erlang:monotonic_time() - TS, native, microseconds),
    case Diff > ?MICROS_PER_UPDATE_MSG of
        true  -> Owner ! {sent, Count + 1},
                 publish(Owner, Client, Count + 1,
                         erlang:monotonic_time());
        false -> publish(Owner, Client, Count + 1, TS)
    end.

recv(Owner, Client0, Count, TS) ->
    {#stomp_frame{body_iolist = Body}, Client1} =
        rabbit_stomp_client:recv(Client0),
    BodyInt = list_to_integer(binary_to_list(iolist_to_binary(Body))),
    Count = BodyInt,
    Diff = erlang:convert_time_unit(
      erlang:monotonic_time() - TS, native, microseconds),
    case Diff > ?MICROS_PER_UPDATE_MSG of
        true  -> Owner ! {recd, Count + 1},
                 recv(Owner, Client1, Count + 1,
                      erlang:monotonic_time());
        false -> recv(Owner, Client1, Count + 1, TS)
    end.

