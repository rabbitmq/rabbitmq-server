%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(stomp).

-export([marshal/2, marshal/3, unmarshal/1]).

-export([list_to_hex/1]).

marshal(Command, Headers) ->
    marshal(Command, Headers, <<>>).
marshal(Command, Headers, Body) ->
    Lines = [Command] ++ [[K, ":", V] || {K, V} <- Headers] ++ [["\n", Body]],
    iolist_to_binary([iolist_join(Lines, "\n"), "\x00"]).

unmarshal(Frame) ->
    [Head, Body] = binary:split(Frame, <<"\n\n">>),
    [Command | HeaderLines] = binary:split(Head, <<"\n">>, [global]),
    Headers = [list_to_tuple(binary:split(Line, <<":">>)) || Line <- HeaderLines],
    [Body1, <<>>] = binary:split(Body, [<<0, 10>>],[{scope,{byte_size(Body)-2, 2}}]),
    {Command, Headers, Body1}.

%% ----------

iolist_join(List, Separator) ->
    lists:reverse(iolist_join2(List, Separator, [])).

iolist_join2([], _Separator, Acc) ->
    Acc;
iolist_join2([E | List], Separator, Acc) ->
    iolist_join2(List, Separator, [E, Separator | Acc]).


list_to_hex(L) ->
    lists:flatten(lists:map(fun(X) -> int_to_hex(X) end, L)).
int_to_hex(N) when N < 256 ->
    [hex(N div 16), hex(N rem 16)].
hex(N) when N < 10 ->
    $0+N;
hex(N) when N >= 10, N < 16 ->
    $a + (N-10).
