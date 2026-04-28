%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(pid_codec_prop_SUITE).

-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(PROP_ITERATIONS, 5000).

-define(TTB_PREFIX,          131).
-define(NEW_PID_EXT,         88).
-define(ATOM_UTF8_EXT,       118).
-define(SMALL_ATOM_UTF8_EXT, 119).

all() ->
    [
     prop_safe_decoder_returns_ok_or_error,
     prop_safe_decoder_accepts_arbitrary_node_bytes,
     prop_safe_decoder_roundtrip_numeric_fields,
     prop_safe_decoder_no_atom_growth,
     prop_pid_from_name_returns_error_when_there_are_no_suitable_candidates,
     prop_pid_from_name_no_atom_growth_well_formed,
     prop_pid_from_name_no_atom_growth_arbitrary_bytes,
     prop_pid_from_name_decodes_well_formed
    ].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

prop_safe_decoder_returns_ok_or_error(_) ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_safe_decoder_returns_ok_or_error_/0, [], ?PROP_ITERATIONS).

prop_safe_decoder_returns_ok_or_error_() ->
    ?FORALL(Bin, binary(),
            case rabbit_pid_codec:decompose_from_binary_safe(Bin) of
                error -> true;
                {ok, #{node := N, id := I, serial := S, creation := C}} ->
                    is_binary(N)
                    andalso is_integer(I) andalso I >= 0
                    andalso is_integer(S) andalso S >= 0
                    andalso is_integer(C) andalso C >= 0
            end).

prop_safe_decoder_accepts_arbitrary_node_bytes(_) ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_safe_decoder_accepts_arbitrary_node_bytes_/0, [],
      ?PROP_ITERATIONS).

prop_safe_decoder_accepts_arbitrary_node_bytes_() ->
    ?FORALL({NodeBytes, ID, Serial, Creation},
            {?LET(L, integer(0, 300), binary(L)),
             uint32(), uint32(), uint32()},
            begin
                Bin = make_pid_bin(NodeBytes, ID, Serial, Creation),
                {ok, #{node := Got, id := GotID,
                       serial := GotS, creation := GotC}} =
                    rabbit_pid_codec:decompose_from_binary_safe(Bin),
                Got =:= NodeBytes
                    andalso GotID =:= ID
                    andalso GotS =:= Serial
                    andalso GotC =:= Creation
            end).

prop_safe_decoder_roundtrip_numeric_fields(_) ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_safe_decoder_roundtrip_numeric_fields_/0, [],
      ?PROP_ITERATIONS).

prop_safe_decoder_roundtrip_numeric_fields_() ->
    Node = node(),
    NodeBin = atom_to_binary(Node),
    ?FORALL({ID, Serial, Creation}, {uint32(), uint32(), uint32()},
            begin
                Parts = #{node => Node, id => ID,
                          serial => Serial, creation => Creation},
                Bin = rabbit_pid_codec:recompose_to_binary(Parts),
                {ok, #{node := NB, id := GotID,
                       serial := GotS, creation := GotC}} =
                    rabbit_pid_codec:decompose_from_binary_safe(Bin),
                NB =:= NodeBin
                    andalso GotID =:= ID
                    andalso GotS =:= Serial
                    andalso GotC =:= Creation
            end).

prop_safe_decoder_no_atom_growth(_) ->
    %% Warm up before sampling the atom count.
    _ = rabbit_pid_codec:decompose_from_binary_safe(<<>>),
    rabbit_ct_proper_helpers:run_proper(
      fun prop_safe_decoder_no_atom_growth_/0, [], ?PROP_ITERATIONS).

prop_safe_decoder_no_atom_growth_() ->
    ?FORALL(Bin, binary(),
            begin
                Before = erlang:system_info(atom_count),
                _ = rabbit_pid_codec:decompose_from_binary_safe(Bin),
                erlang:system_info(atom_count) =:= Before
            end).

prop_pid_from_name_returns_error_when_there_are_no_suitable_candidates(_) ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_pid_from_name_returns_error_when_there_are_no_suitable_candidates_/0,
      [], ?PROP_ITERATIONS).

prop_pid_from_name_returns_error_when_there_are_no_suitable_candidates_() ->
    %% With no candidate nodes, every input must yield 'error'.
    ?FORALL(Bin, binary(),
            error =:= rabbit_volatile_queue:pid_from_name(
                        <<"amq.rabbitmq.reply-to.", Bin/binary>>, #{})).

prop_pid_from_name_no_atom_growth_well_formed(_) ->
    %% Warm up before sampling the atom count.
    _ = rabbit_volatile_queue:pid_from_name(
          <<"amq.rabbitmq.reply-to.", (base64:encode(<<>>))/binary, ".k">>,
          #{}),
    rabbit_ct_proper_helpers:run_proper(
      fun prop_pid_from_name_no_atom_growth_well_formed_/0, [],
      ?PROP_ITERATIONS).

prop_pid_from_name_no_atom_growth_well_formed_() ->
    ?FORALL({NodeBytes, ID, Serial, Creation, Suffix},
            {?LET(L, integer(0, 200), binary(L)),
             uint32(), uint32(), uint32(),
             ?LET(L, integer(1, 50), binary(L))},
            begin
                QName = craft_reply_to_with_suffix(NodeBytes, ID, Serial,
                                                   Creation, Suffix),
                Before = erlang:system_info(atom_count),
                Result = rabbit_volatile_queue:pid_from_name(QName, #{}),
                After = erlang:system_info(atom_count),
                After =:= Before andalso Result =:= error
            end).

prop_pid_from_name_no_atom_growth_arbitrary_bytes(_) ->
    _ = rabbit_volatile_queue:pid_from_name(
          <<"amq.rabbitmq.reply-to.">>, #{}),
    rabbit_ct_proper_helpers:run_proper(
      fun prop_pid_from_name_no_atom_growth_arbitrary_bytes_/0, [],
      ?PROP_ITERATIONS).

prop_pid_from_name_no_atom_growth_arbitrary_bytes_() ->
    ?FORALL(Bytes, binary(),
            begin
                QName = <<"amq.rabbitmq.reply-to.", Bytes/binary>>,
                Before = erlang:system_info(atom_count),
                _ = rabbit_volatile_queue:pid_from_name(QName, #{}),
                erlang:system_info(atom_count) =:= Before
            end).

prop_pid_from_name_decodes_well_formed(_) ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_pid_from_name_decodes_well_formed_/0, [], ?PROP_ITERATIONS).

prop_pid_from_name_decodes_well_formed_() ->
    Hash = 4242,
    Synthetic = rabbit_nodes_common:make("reply", integer_to_list(Hash)),
    SyntheticBin = atom_to_binary(Synthetic),
    Candidate = node(),
    Candidates = #{Hash => Candidate},
    ?FORALL({ID, Serial, Creation, Suffix},
            {uint32(), uint32(), uint32(),
             ?LET(L, integer(1, 30), binary(L))},
            begin
                PidBin = make_pid_bin(SyntheticBin, ID, Serial, Creation),
                QName = <<"amq.rabbitmq.reply-to.",
                          (base64:encode(PidBin))/binary, ".", Suffix/binary>>,
                {ok, Pid} = rabbit_volatile_queue:pid_from_name(QName, Candidates),
                node(Pid) =:= Candidate
            end).

%%% Helpers

uint32() -> integer(0, 16#FFFFFFFF).

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

craft_reply_to_with_suffix(NodeBytes, ID, Serial, Creation, Suffix) ->
    PidBin = make_pid_bin(NodeBytes, ID, Serial, Creation),
    <<"amq.rabbitmq.reply-to.", (base64:encode(PidBin))/binary,
      ".", Suffix/binary>>.
