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

%% A very simple publish-and-consume-as-fast-as-you-can test.

run() ->
    [put(K, 0) || K <- [sent, recd, last_sent, last_recd]],
    put(last_ts, erlang:now()),
    {ok, Pub} = rabbit_stomp_client:connect(),
    {ok, Recv} = rabbit_stomp_client:connect(),
    Self = self(),
    spawn(fun() -> publish(Self, Pub, 0, erlang:now()) end),
    rabbit_stomp_client:send(
      Recv, "SUBSCRIBE", [{"destination", ?DESTINATION}]),
    spawn(fun() -> recv(Self, Recv, 0, erlang:now()) end),
    report().

report() ->
    receive
        {sent, C} -> put(sent, get(sent) + C);
        {recd, C} -> put(recd, get(recd) + C)
    end,
    Diff = timer:now_diff(erlang:now(), get(last_ts)),
    case Diff > ?MICROS_PER_UPDATE of
        true  -> S = get(sent) - get(last_sent),
                 R = get(recd) - get(last_recd),
                 put(last_sent, get(sent)),
                 put(last_recd, get(recd)),
                 put(last_ts, erlang:now()),
                 io:format("Send ~p msg/s | Recv ~p msg/s~n",
                           [trunc(S * ?MICROS_PER_UPDATE / Diff),
                            trunc(R * ?MICROS_PER_UPDATE / Diff)]);
        false -> ok
    end,
    report().

publish(Owner, Sock, Count, TS) ->
    rabbit_stomp_client:send(
      Sock, "SEND", [{"destination", ?DESTINATION}], ["hello"]),
    Diff = timer:now_diff(erlang:now(), TS),
    case Diff > ?MICROS_PER_UPDATE_MSG of
        true  -> Owner ! {sent, Count},
                 publish(Owner, Sock, 0, erlang:now());
        false -> publish(Owner, Sock, Count + 1, TS)
    end.

recv(Owner, Sock, Count, TS) ->
    #stomp_frame{} = rabbit_stomp_client:recv(Sock),
    Diff = timer:now_diff(erlang:now(), TS),
    case Diff > ?MICROS_PER_UPDATE_MSG of
        true  -> Owner ! {recd, Count},
                 recv(Owner, Sock, 0, erlang:now());
        false -> recv(Owner, Sock, Count + 1, TS)
    end.

