%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(pid_recomposition).

%% API
-export([
    to_binary/1,
    from_binary/1,
    decompose/1,
    recompose/1
]).

-define(TTB_PREFIX, 131).

-define(NEW_PID_EXT, 88).
-define(ATOM_UTF8_EXT, 118).
-define(SMALL_ATOM_UTF8_EXT, 119).

-spec decompose(pid()) -> #{atom() => any()}.
decompose(Pid) ->
    from_binary(term_to_binary(Pid, [{minor_version, 2}])).

-spec from_binary(binary()) -> #{atom() => any()}.
from_binary(Bin) ->
    <<?TTB_PREFIX, ?NEW_PID_EXT, PidData/binary>> = Bin,
    {Node, Rest2} = case PidData of
        <<?ATOM_UTF8_EXT, AtomLen:16/integer, Node0:AtomLen/binary, Rest1/binary>> ->
            {Node0, Rest1};
        <<?SMALL_ATOM_UTF8_EXT, AtomLen/integer, Node0:AtomLen/binary, Rest1/binary>> ->
            {Node0, Rest1}
    end,
    <<ID:32/integer, Serial:32/integer, Creation:32/integer>> = Rest2,
    #{
        node     => binary_to_atom(Node, utf8),
        id       => ID,
        serial   => Serial,
        creation => Creation
    }.

-spec to_binary(#{atom() => any()}) -> binary().
to_binary(#{node := Node, id := ID, serial := Serial, creation := Creation}) ->
    BinNode = atom_to_binary(Node),
    NodeLen = byte_size(BinNode),
    <<?TTB_PREFIX:8/unsigned,
      ?NEW_PID_EXT:8/unsigned,
      ?ATOM_UTF8_EXT:8/unsigned,
      NodeLen:16/unsigned,
      BinNode/binary,
      ID:32,
      Serial:32,
      Creation:32>>.

-spec recompose(#{atom() => any()}) -> pid().
recompose(M) ->
    binary_to_term(to_binary(M)).
