%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% Deterministic map operations.
-module(rabbit_fifo_maps).

-export([keys/2,
         fold/4,
         iterator/2]).

-spec keys(Map, ra_machine:version()) -> Keys when
    Map :: #{Key => _},
    Keys :: [Key].
keys(Map, Vsn) ->
    Keys = maps:keys(Map),
    case is_deterministic(Vsn) of
        true ->
            lists:sort(Keys);
        false ->
            Keys
    end.

-spec fold(Fun, Init, Map, ra_machine:version()) -> Acc when
    Fun :: fun((Key, Value, AccIn) -> AccOut),
    Init :: term(),
    Acc :: AccOut,
    AccIn :: Init | AccOut,
    Map :: #{Key => Value}.
fold(Fun, Init, Map, Vsn)  ->
    maps:fold(Fun, Init, iterator(Map, Vsn)).

-spec iterator(Map, ra_machine:version()) -> Iterator when
    Map :: #{Key => Value},
    Iterator :: maps:iterator(Key, Value).
iterator(Map, Vsn) ->
    case is_deterministic(Vsn) of
        true ->
            maps:iterator(Map, ordered);
        false ->
            maps:iterator(Map)
    end.

is_deterministic(Vsn) when is_integer(Vsn) ->
    Vsn > 5.
