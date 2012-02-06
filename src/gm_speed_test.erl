%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(gm_speed_test).

-export([test/3]).
-export([joined/2, members_changed/3, handle_msg/3, terminate/2]).
-export([wile_e_coyote/2]).

-behaviour(gm).

-include("gm_specs.hrl").

%% callbacks

joined(Owner, _Members) ->
    Owner ! joined,
    ok.

members_changed(_Owner, _Births, _Deaths) ->
    ok.

handle_msg(Owner, _From, ping) ->
    Owner ! ping,
    ok.

terminate(Owner, _Reason) ->
    Owner ! terminated,
    ok.

%% other

wile_e_coyote(Time, WriteUnit) ->
    {ok, Pid} = gm:start_link(?MODULE, ?MODULE, self()),
    receive joined -> ok end,
    timer:sleep(1000), %% wait for all to join
    timer:send_after(Time, stop),
    Start = now(),
    {Sent, Received} = loop(Pid, WriteUnit, 0, 0),
    End = now(),
    ok = gm:leave(Pid),
    receive terminated -> ok end,
    Elapsed = timer:now_diff(End, Start) / 1000000,
    io:format("Sending rate:   ~p msgs/sec~nReceiving rate: ~p msgs/sec~n~n",
              [Sent/Elapsed, Received/Elapsed]),
    ok.

loop(Pid, WriteUnit, Sent, Received) ->
    case read(Received) of
        {stop, Received1} -> {Sent, Received1};
        {ok,   Received1} -> ok = write(Pid, WriteUnit),
                             loop(Pid, WriteUnit, Sent + WriteUnit, Received1)
    end.

read(Count) ->
    receive
        ping -> read(Count + 1);
        stop -> {stop, Count}
    after 5 ->
            {ok, Count}
    end.

write(_Pid, 0) -> ok;
write(Pid,  N) -> ok = gm:broadcast(Pid, ping),
                  write(Pid, N - 1).

test(Time, WriteUnit, Nodes) ->
    ok = gm:create_tables(),
    [spawn(Node, ?MODULE, wile_e_coyote, [Time, WriteUnit]) || Node <- Nodes].
