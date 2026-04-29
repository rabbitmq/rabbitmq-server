%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_pid_codec).

-export([decompose/1,
         decompose_from_binary/1,
         recompose/1,
         recompose_to_binary/1]).

-define(NEW_PID_EXT, 88).
-define(ATOM_UTF8_EXT, 118).
-define(SMALL_ATOM_UTF8_EXT, 119).
-define(TTB_PREFIX, 131).

-type decomposed_pid() :: #{node := node(),
                            id := non_neg_integer(),
                            serial := non_neg_integer(),
                            creation := non_neg_integer()}.

-type decomposed_pid_bin() :: #{node := binary(),
                                id := non_neg_integer(),
                                serial := non_neg_integer(),
                                creation := non_neg_integer()}.

-export_type([decomposed_pid/0, decomposed_pid_bin/0]).

-spec decompose(pid()) -> decomposed_pid().
decompose(Pid) ->
    Bin = term_to_binary(Pid, [{minor_version, 2}]),
    {ok, #{node := NodeBin} = Parts} = decompose_from_binary(Bin),
    %% Safe: the input is a real local pid, so its node atom always exists.
    Parts#{node := binary_to_existing_atom(NodeBin, utf8)}.

%% Returns the node name as a binary and 'error' on malformed input.
-spec decompose_from_binary(binary()) -> {ok, decomposed_pid_bin()} | error.
decompose_from_binary(<<?TTB_PREFIX, ?NEW_PID_EXT,
                        ?ATOM_UTF8_EXT, Len:16/integer, Node:Len/binary,
                        ID:32/integer, Serial:32/integer,
                        Creation:32/integer>>) ->
    {ok,
      #{node => Node,
        id => ID,
        serial => Serial,
        creation => Creation}};
decompose_from_binary(<<?TTB_PREFIX, ?NEW_PID_EXT,
                        ?SMALL_ATOM_UTF8_EXT, Len:8/integer, Node:Len/binary,
                        ID:32/integer, Serial:32/integer,
                        Creation:32/integer>>) ->
    {ok,
      #{node => Node,
        id => ID,
        serial => Serial,
        creation => Creation}};
decompose_from_binary(_) ->
    error.

-spec recompose_to_binary(decomposed_pid()) -> binary().
recompose_to_binary(#{node := Node,
                      id := ID,
                      serial := Serial,
                      creation := Creation}) ->
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

-spec recompose(decomposed_pid()) -> pid().
recompose(Map) ->
    Bin = recompose_to_binary(Map),
    binary_to_term(Bin).
