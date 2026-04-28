%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(pid_codec_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TTB_PREFIX,          131).
-define(NEW_PID_EXT,         88).
-define(ATOM_UTF8_EXT,       118).
-define(SMALL_ATOM_UTF8_EXT, 119).

all() ->
    [
     safe_decoder_roundtrip_self,
     safe_decoder_returns_binary_node,
     safe_decoder_rejects_malformed,
     safe_decoder_no_atoms_for_unknown_node_names,
     safe_decoder_handles_long_node_names,
     pid_from_name_no_atoms_for_crafted_input,
     pid_from_name_rejects_malformed_node_name,
     pid_from_name_happy_path
    ].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

safe_decoder_roundtrip_self(_) ->
    Pid = self(),
    Bin = term_to_binary(Pid, [{minor_version, 2}]),
    {ok, Parts} = rabbit_pid_codec:decompose_from_binary_safe(Bin),
    Recomposed = rabbit_pid_codec:recompose(Parts#{node := node()}),
    ?assertEqual(Pid, Recomposed).

safe_decoder_returns_binary_node(_) ->
    Bin = term_to_binary(self(), [{minor_version, 2}]),
    {ok, #{node := NodeBin}} = rabbit_pid_codec:decompose_from_binary_safe(Bin),
    ?assert(is_binary(NodeBin)),
    ?assertEqual(atom_to_binary(node()), NodeBin).

safe_decoder_rejects_malformed(_) ->
    Cases = [
             <<>>,
             <<0>>,
             <<?TTB_PREFIX>>,
             <<?TTB_PREFIX, 0>>,
             <<?TTB_PREFIX, ?NEW_PID_EXT>>,
             <<?TTB_PREFIX, ?NEW_PID_EXT, ?ATOM_UTF8_EXT>>,
             <<?TTB_PREFIX, ?NEW_PID_EXT, ?ATOM_UTF8_EXT, 0:16, 0:32, 0:32>>,
             <<(make_pid_bin(<<"x@1">>, 0, 0, 0))/binary, "trailing">>,
             <<?TTB_PREFIX, ?NEW_PID_EXT, ?ATOM_UTF8_EXT, 10:16, "abc",
               0:32, 0:32, 0:32>>,
             %% The legacy PID_EXT tag (103) is not accepted.
             <<?TTB_PREFIX, 103, 0, 0, 0, 0>>,
             %% TTB terms that are not pids.
             term_to_binary(an_atom),
             term_to_binary({a, tuple}),
             term_to_binary(42),
             %% Correct tags but the body is one byte short.
             <<?TTB_PREFIX, ?NEW_PID_EXT,
               ?SMALL_ATOM_UTF8_EXT, 1:8, "x", 0:32, 0:32, 0:24>>
            ],
    [?assertEqual(error, rabbit_pid_codec:decompose_from_binary_safe(C))
     || C <- Cases].

safe_decoder_no_atoms_for_unknown_node_names(_) ->
    Inputs = [{fresh_node_bytes(I), I} || I <- lists:seq(1, 1000)],
    Bins = [{Name, make_pid_bin(Name, I, I, I)} || {Name, I} <- Inputs],
    %% Warm up before sampling the atom count.
    {_, B0} = hd(Bins),
    {ok, _} = rabbit_pid_codec:decompose_from_binary_safe(B0),
    Before = erlang:system_info(atom_count),
    lists:foreach(
      fun({Name, B}) ->
              {ok, #{node := Got}} =
                  rabbit_pid_codec:decompose_from_binary_safe(B),
              ?assertEqual(Name, Got)
      end,
      Bins),
    ?assertEqual(Before, erlang:system_info(atom_count)).

safe_decoder_handles_long_node_names(_) ->
    %% Names longer than 255 bytes use the ATOM_UTF8_EXT form.
    Long = binary:copy(<<"a">>, 65535),
    Bin = make_pid_bin(Long, 1, 2, 3),
    {ok, #{node := Got, id := 1, serial := 2, creation := 3}} =
        rabbit_pid_codec:decompose_from_binary_safe(Bin),
    ?assertEqual(Long, Got).

pid_from_name_no_atoms_for_crafted_input(_) ->
    CandidateNodes = #{},
    QNames = [craft_reply_to(fresh_node_bytes(I), I) || I <- lists:seq(1, 1000)],
    %% Warm up before sampling the atom count.
    error = rabbit_volatile_queue:pid_from_name(hd(QNames), CandidateNodes),
    Before = erlang:system_info(atom_count),
    lists:foreach(
      fun(QName) ->
              ?assertEqual(error,
                           rabbit_volatile_queue:pid_from_name(QName,
                                                               CandidateNodes))
      end,
      QNames),
    ?assertEqual(Before, erlang:system_info(atom_count)).

pid_from_name_rejects_malformed_node_name(_) ->
    Names = [<<"noatsign">>,
             <<"x@notdigits">>,
             <<"x@">>,
             <<"@123">>,
             <<>>,
             <<0, 1, 2, 3>>],
    [begin
         QName = craft_reply_to(N, 0),
         ?assertEqual(error,
                      rabbit_volatile_queue:pid_from_name(QName, #{}))
     end || N <- Names].

pid_from_name_happy_path(_) ->
    Hash = 4242,
    SyntheticNode = rabbit_nodes_common:make("reply", integer_to_list(Hash)),
    SyntheticNodeBin = atom_to_binary(SyntheticNode),
    PidBin = make_pid_bin(SyntheticNodeBin, 7, 0, 0),
    QName = <<"amq.rabbitmq.reply-to.",
              (base64:encode(PidBin))/binary, ".somekey">>,
    Candidate = node(),
    {ok, Pid} = rabbit_volatile_queue:pid_from_name(QName, #{Hash => Candidate}),
    ?assertEqual(Candidate, node(Pid)).

%%% Helpers

make_pid_bin(NodeBin, ID, Serial, Creation) ->
    NodeLen = byte_size(NodeBin),
    case NodeLen =< 255 of
        true ->
            <<?TTB_PREFIX, ?NEW_PID_EXT,
              ?SMALL_ATOM_UTF8_EXT, NodeLen:8, NodeBin/binary,
              ID:32, Serial:32, Creation:32>>;
        false ->
            <<?TTB_PREFIX, ?NEW_PID_EXT,
              ?ATOM_UTF8_EXT, NodeLen:16, NodeBin/binary,
              ID:32, Serial:32, Creation:32>>
    end.

fresh_node_bytes(I) ->
    Uniq = erlang:unique_integer([positive]),
    iolist_to_binary(io_lib:format("codec-test-~b-~b@x", [Uniq, I])).

craft_reply_to(NodeBytes, ID) ->
    PidBin = make_pid_bin(NodeBytes, ID, 0, 0),
    <<"amq.rabbitmq.reply-to.", (base64:encode(PidBin))/binary, ".k">>.
