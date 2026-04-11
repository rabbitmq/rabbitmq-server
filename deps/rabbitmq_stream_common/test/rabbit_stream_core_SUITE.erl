-module(rabbit_stream_core_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("rabbit_stream.hrl").

-import(rabbit_ct_proper_helpers, [run_proper/3]).

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [{group, tests}].

suite() ->
    [{timetrap, {seconds, 30}}].

groups() ->
    [{tests, [],
      [roundtrip, roundtrip_metadata, roundtrip_metadata_no_leader,
       zero_size_frame_does_not_crash,
       frame_size_enforcement,
       frame_size_enforcement_partial,
       frame_size_enforcement_unlimited,
       frame_size_enforcement_discards_after_error,
       frame_size_enforcement_boundary,
       frame_size_enforcement_chunked_header,
       frame_size_enforcement_valid_then_oversized,
       frame_size_enforcement_zero_size_frame,
       set_frame_max_tightens_limit,
       set_frame_max_allows_in_flight_frame_to_complete,
       set_frame_max_to_unlimited,
       init_with_zero_frame_max_means_unlimited,
       set_frame_max_to_zero_means_unlimited,
       set_frame_max_preserves_pending_commands,
       prop_frame_within_limit_accepted,
       prop_frame_exceeding_limit_rejected,
       prop_unlimited_accepts_any_size,
       prop_boundary_exact_limit_accepted,
       prop_chunked_data_same_result,
       prop_set_frame_max_matches_init]}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

group(_GroupName) ->
    [].

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

roundtrip(_Config) ->
    test_roundtrip({publish, 42, 1, <<"payload">>}),
    test_roundtrip({publish_v2, 42, 1, <<"payload">>}),
    test_roundtrip({publish_confirm, 42, [1, 2, 3]}),

    test_roundtrip({publish_error,
                    42,
                    ?RESPONSE_CODE_STREAM_DOES_NOT_EXIST,
                    [2, 3, 4]}),
    test_roundtrip({deliver, 53, <<"chunk">>}),
    test_roundtrip({deliver_v2, 53, 10, <<"chunk">>}),
    test_roundtrip({credit, 53, 12}),
    test_roundtrip({metadata_update, <<"stream1">>,
                    ?RESPONSE_VHOST_ACCESS_FAILURE}),
    test_roundtrip({store_offset, <<"offset_ref">>, <<"stream">>, 12}),
    test_roundtrip(heartbeat),
    test_roundtrip({tune, 53, 12}),
    %% REQUESTS
    test_roundtrip({request, 99,
                    {declare_publisher,
                     42,
                     <<"writer_ref">>,
                     <<"stream_name">>}}),
    test_roundtrip({request, 99,
                    {query_publisher_sequence, <<"writer_ref">>,
                     <<"stream_name">>}}),
    test_roundtrip({request, 99, {delete_publisher, 42}}),
    [test_roundtrip({request, 99,
                     {subscribe, 53, <<"stream_name">>, Spec, 12, #{}}})
     || Spec
            <- [last,
                next,
                first,
                65432,
                {timestamp, erlang:system_time(millisecond)}]],
    test_roundtrip({request, 99,
                    {query_offset, <<"offset_ref">>, <<"stream">>}}),
    test_roundtrip({request, 99, {unsubscribe, 53}}),
    Args = #{<<"arg1">> => <<"arg1_value">>},
    test_roundtrip({request, 99,
                    {create_stream, <<"stream_name">>, Args}}),
    test_roundtrip({request, 99,
                    {create_stream, <<"stream_name">>, #{}}}),
    test_roundtrip({request, 99, {delete_stream, <<"stream_name">>}}),
    test_roundtrip({request, 99,
                    {metadata, [<<"stream1">>, <<"stream2">>]}}),
    test_roundtrip({request, 99,
                    {peer_properties, #{<<"k1">> => <<"v1">>}}}),
    test_roundtrip({request, 99, sasl_handshake}),
    test_roundtrip({request, 99,
                    {sasl_authenticate, <<"mechanism">>, <<>>}}),
    test_roundtrip({request, 99,
                    {sasl_authenticate, <<"mechanism">>, <<"challenge">>}}),
    test_roundtrip({request, 99, {open, <<"vhost">>}}),
    test_roundtrip({request, 99, {close, 99, <<"reason">>}}),
    test_roundtrip({request, 99, {route, <<"rkey.*">>, <<"exchange">>}}),
    test_roundtrip({request, 99, {partitions, <<"super stream">>}}),
    test_roundtrip({request, 99, {consumer_update, 1, true}}),
    test_roundtrip({request, 99,
                    {exchange_command_versions,
                     [{deliver, ?VERSION_1, ?VERSION_1}]}}),
    test_roundtrip({request, 99, {stream_stats, <<"stream_name">>}}),
    test_roundtrip({request, 99,
                    {create_super_stream, <<"hello">>,
                     [<<"stream1">>, <<"stream2">>, <<"stream3">>], [<<"bk1">>, <<"bk2">>, <<"bk3">>],
                     Args}}),
    test_roundtrip({request, 99,
                    {create_super_stream, <<"super_stream_name">>,
                     [<<"stream1">>, <<"stream2">>, <<"stream3">>], [<<"bk1">>, <<"bk2">>, <<"bk3">>],
                     #{}}}),
    test_roundtrip({request, 99, {delete_super_stream, <<"super_stream_name">>}}),
    [test_roundtrip({request, 99,
                     {resolve_offset_spec, <<"stream_name">>, Spec, #{}}})
     || Spec
            <- [last,
                next,
                first,
                65432,
                {timestamp, erlang:system_time(millisecond)}]],
    test_roundtrip({request, 99,
                    {resolve_offset_spec, <<"stream_name">>, first,
                     #{<<"filter.0">> => <<"value0">>}}}),

    %% RESPONSES
    [test_roundtrip({response, 99, {Tag, 53}})
     || Tag
            <- [declare_publisher,
                delete_publisher,
                subscribe,
                unsubscribe,
                create_stream,
                delete_stream,
                create_super_stream,
                delete_super_stream,
                open,
                close]],

    test_roundtrip({response, 99, {query_publisher_sequence, 98, 1234}}),
    test_roundtrip({response, 99, {query_offset, 1, 12}}),
    test_roundtrip({response, 99, {resolve_offset_spec, 1, ?OFFSET_TYPE_OFFSET, 12345}}),

    test_roundtrip({response, 99,
                    {peer_properties, 1, #{<<"k1">> => <<"v1">>}}}),

    test_roundtrip({response, 99,
                    {sasl_handshake, 1, [<<"m1">>, <<"m2">>]}}),
    test_roundtrip({response, 99,
                    {sasl_authenticate, 1, <<"challenge">>}}),
    test_roundtrip({response, 0, {tune, 10000, 12345}}),
    %  %% NB: does not write correlation id
    test_roundtrip({response, 0, {credit, 98, 200}}),
    test_roundtrip({response, 99,
                    {route, 1, [<<"stream1">>, <<"stream2">>]}}),
    test_roundtrip({response, 99,
                    {partitions, 1, [<<"stream1">>, <<"stream2">>]}}),
    test_roundtrip({response, 99, {consumer_update, 1, none}}),
    test_roundtrip({response, 99,
                    {exchange_command_versions, 1,
                     [{publish, ?VERSION_1, ?VERSION_1}]}}),
    test_roundtrip({response, 99,
                    {stream_stats, 1, #{<<"committed_offset">> => 42}}}),
    ok.

roundtrip_metadata(_Config) ->
    Host1 = {<<"host1">>, 1234},
    Host2 = {<<"host2">>, 1235},
    Host3 = {<<"host3">>, 1236},
    Endpoints = [Host1, Host2, Host3],
    Metadata =
        #{<<"stream1">> => {Host1, [Host2, Host3]},
          <<"stream2">> => stream_not_found,
          <<"stream3">> => stream_not_available},
    test_roundtrip({response, 1, {metadata, Endpoints, Metadata}}),
    ok.

roundtrip_metadata_no_leader(_Config) ->
    Host1 = {<<"host1">>, 1234},
    Host2 = {<<"host2">>, 1235},
    Host3 = {<<"host3">>, 1236},
    Endpoints = [Host1, Host2, Host3],
    Metadata =
        #{<<"stream1">> => {undefined, [Host2, Host3]},
          <<"stream2">> => stream_not_found,
          <<"stream3">> => stream_not_available},
    Cmd = {response, 1, {metadata, Endpoints, Metadata}},
    test_roundtrip(Cmd),
    ok.

%% A zero-size frame (e.g. sent by a port scanner) must not crash the parser.
zero_size_frame_does_not_crash(_Config) ->
    Init = rabbit_stream_core:init(undefined),
    %% <<0,0,0,0>> is a valid frame header with size 0, producing an empty frame body
    Data = <<0,0,0,0, 0,0,0,0>>,
    State = rabbit_stream_core:incoming_data(Data, Init),
    {[{unknown, <<>>}, {unknown, <<>>}], _} = rabbit_stream_core:all_commands(State),
    ok.

test_roundtrip(Cmd) ->
    Init = rabbit_stream_core:init(undefined),
    Frame = iolist_to_binary(rabbit_stream_core:frame(Cmd)),
    {[Cmd], _} =
        rabbit_stream_core:all_commands(
            rabbit_stream_core:incoming_data(Frame, Init)),
    ok.

frame_size_enforcement(_Config) ->
    FrameMax = 100,
    Init = rabbit_stream_core:init(#{frame_max => FrameMax}),
    %% Frame within limit should be accepted
    SmallCmd = heartbeat,
    SmallFrame = iolist_to_binary(rabbit_stream_core:frame(SmallCmd)),
    {[SmallCmd], _} =
        rabbit_stream_core:all_commands(
            rabbit_stream_core:incoming_data(SmallFrame, Init)),
    %% Frame exceeding limit should produce frame_too_large error
    OversizedPayload = binary:copy(<<0>>, FrameMax + 50),
    OversizedSize = byte_size(OversizedPayload),
    OversizedData = <<OversizedSize:32, OversizedPayload/binary>>,
    State = rabbit_stream_core:incoming_data(OversizedData, Init),
    {[{frame_too_large, OversizedSize, FrameMax}], _} =
        rabbit_stream_core:all_commands(State),
    ok.

frame_size_enforcement_partial(_Config) ->
    FrameMax = 100,
    Init = rabbit_stream_core:init(#{frame_max => FrameMax}),
    %% Declare a frame larger than FrameMax but only send header
    DeclaredSize = FrameMax + 500,
    Header = <<DeclaredSize:32>>,
    State = rabbit_stream_core:incoming_data(Header, Init),
    {[{frame_too_large, DeclaredSize, FrameMax}], _} =
        rabbit_stream_core:all_commands(State),
    ok.

frame_size_enforcement_unlimited(_Config) ->
    Init = rabbit_stream_core:init(#{frame_max => unlimited}),
    %% Large frame should be accepted when unlimited
    LargePayload = binary:copy(<<0>>, 10000),
    LargeSize = byte_size(LargePayload),
    LargeData = <<LargeSize:32, LargePayload/binary>>,
    State = rabbit_stream_core:incoming_data(LargeData, Init),
    {[{unknown, LargePayload}], _} = rabbit_stream_core:all_commands(State),
    ok.

frame_size_enforcement_discards_after_error(_Config) ->
    FrameMax = 100,
    Init = rabbit_stream_core:init(#{frame_max => FrameMax}),
    %% Trigger frame_too_large error
    DeclaredSize = FrameMax + 500,
    Header = <<DeclaredSize:32>>,
    State1 = rabbit_stream_core:incoming_data(Header, Init),
    {[{frame_too_large, DeclaredSize, FrameMax}], State2} =
        rabbit_stream_core:all_commands(State1),
    %% Subsequent data should be discarded
    MoreData = binary:copy(<<1>>, 1000),
    State3 = rabbit_stream_core:incoming_data(MoreData, State2),
    {[], _} = rabbit_stream_core:all_commands(State3),
    ok.

frame_size_enforcement_boundary(_Config) ->
    FrameMax = 100,
    Init = rabbit_stream_core:init(#{frame_max => FrameMax}),
    %% Frame exactly at limit should be accepted
    ExactPayload = binary:copy(<<0>>, FrameMax),
    ExactData = <<FrameMax:32, ExactPayload/binary>>,
    State1 = rabbit_stream_core:incoming_data(ExactData, Init),
    {[{unknown, ExactPayload}], _} = rabbit_stream_core:all_commands(State1),
    %% Frame one byte over limit should be rejected
    OverByOne = FrameMax + 1,
    OverPayload = binary:copy(<<0>>, OverByOne),
    OverData = <<OverByOne:32, OverPayload/binary>>,
    State2 = rabbit_stream_core:incoming_data(OverData, Init),
    {[{frame_too_large, OverByOne, FrameMax}], _} =
        rabbit_stream_core:all_commands(State2),
    ok.

frame_size_enforcement_chunked_header(_Config) ->
    FrameMax = 100,
    Init = rabbit_stream_core:init(#{frame_max => FrameMax}),
    %% Send header in chunks (1 byte, then 3 bytes) for oversized frame
    OversizedSize = FrameMax + 500,
    <<B1:1/binary, B2:3/binary>> = <<OversizedSize:32>>,
    State1 = rabbit_stream_core:incoming_data(B1, Init),
    {[], State2} = rabbit_stream_core:all_commands(State1),
    State3 = rabbit_stream_core:incoming_data(B2, State2),
    {[{frame_too_large, OversizedSize, FrameMax}], _} =
        rabbit_stream_core:all_commands(State3),
    ok.

frame_size_enforcement_valid_then_oversized(_Config) ->
    FrameMax = 100,
    Init = rabbit_stream_core:init(#{frame_max => FrameMax}),
    %% Send a valid heartbeat frame followed by an oversized frame
    ValidFrame = iolist_to_binary(rabbit_stream_core:frame(heartbeat)),
    OversizedSize = FrameMax + 500,
    OversizedHeader = <<OversizedSize:32>>,
    CombinedData = <<ValidFrame/binary, OversizedHeader/binary>>,
    State = rabbit_stream_core:incoming_data(CombinedData, Init),
    {[heartbeat, {frame_too_large, OversizedSize, FrameMax}], _} =
        rabbit_stream_core:all_commands(State),
    ok.

frame_size_enforcement_zero_size_frame(_Config) ->
    FrameMax = 100,
    Init = rabbit_stream_core:init(#{frame_max => FrameMax}),
    %% Zero-size frame should be accepted (0 <= FrameMax)
    ZeroSizeFrame = <<0:32>>,
    State = rabbit_stream_core:incoming_data(ZeroSizeFrame, Init),
    {[{unknown, <<>>}], _} = rabbit_stream_core:all_commands(State),
    ok.

set_frame_max_tightens_limit(_Config) ->
    InitialMax = 1000,
    TightenedMax = 100,
    Init = rabbit_stream_core:init(#{frame_max => InitialMax}),
    Payload500 = binary:copy(<<0>>, 500),
    Data500 = <<500:32, Payload500/binary>>,
    State0 = rabbit_stream_core:incoming_data(Data500, Init),
    {[{unknown, Payload500}], State1} =
        rabbit_stream_core:all_commands(State0),
    State2 = rabbit_stream_core:set_frame_max(TightenedMax, State1),
    State3 = rabbit_stream_core:incoming_data(Data500, State2),
    {[{frame_too_large, 500, TightenedMax}], _} =
        rabbit_stream_core:all_commands(State3),
    ok.

set_frame_max_allows_in_flight_frame_to_complete(_Config) ->
    InitialMax = 1000,
    TightenedMax = 50,
    Init = rabbit_stream_core:init(#{frame_max => InitialMax}),
    %% Start a 500-byte frame whose header was accepted under the
    %% initial limit but whose payload has not fully arrived.
    FullPayload = binary:copy(<<0>>, 500),
    <<FirstHalf:250/binary, SecondHalf/binary>> = FullPayload,
    State0 =
        rabbit_stream_core:incoming_data(<<500:32, FirstHalf/binary>>, Init),
    %% Tightening the limit mid-frame must not retroactively reject an
    %% already-accepted header. The remaining bytes complete the frame.
    State1 = rabbit_stream_core:set_frame_max(TightenedMax, State0),
    State2 = rabbit_stream_core:incoming_data(SecondHalf, State1),
    {[{unknown, FullPayload}], _} = rabbit_stream_core:all_commands(State2),
    ok.

set_frame_max_to_unlimited(_Config) ->
    Init = rabbit_stream_core:init(#{frame_max => 100}),
    State1 = rabbit_stream_core:set_frame_max(unlimited, Init),
    LargePayload = binary:copy(<<0>>, 5000),
    LargeData = <<5000:32, LargePayload/binary>>,
    State2 = rabbit_stream_core:incoming_data(LargeData, State1),
    {[{unknown, LargePayload}], _} = rabbit_stream_core:all_commands(State2),
    ok.

%% The protocol convention is that frame_max = 0 means "no limit",
%% as historically has been the case in RabbitMQ, AMQP 0-9-1.
%% `init/1` must translate 0 to the parser's 'unlimited' sentinel,
%% otherwise the parser would reject every frame as too large.
init_with_zero_frame_max_means_unlimited(_Config) ->
    Init = rabbit_stream_core:init(#{frame_max => 0}),
    LargePayload = binary:copy(<<0>>, 5000),
    LargeData = <<5000:32, LargePayload/binary>>,
    State = rabbit_stream_core:incoming_data(LargeData, Init),
    {[{unknown, LargePayload}], _} = rabbit_stream_core:all_commands(State),
    ok.

%% Same contract as above for `set_frame_max/2`: a post-TUNE update
%% to 0 must resolve to "unlimited", not a 0-byte ceiling.
set_frame_max_to_zero_means_unlimited(_Config) ->
    Init = rabbit_stream_core:init(#{frame_max => 100}),
    State1 = rabbit_stream_core:set_frame_max(0, Init),
    LargePayload = binary:copy(<<0>>, 5000),
    LargeData = <<5000:32, LargePayload/binary>>,
    State2 = rabbit_stream_core:incoming_data(LargeData, State1),
    {[{unknown, LargePayload}], _} = rabbit_stream_core:all_commands(State2),
    ok.

set_frame_max_preserves_pending_commands(_Config) ->
    InitialMax = 1000,
    TightenedMax = 50,
    Init = rabbit_stream_core:init(#{frame_max => InitialMax}),
    ValidFrame = iolist_to_binary(rabbit_stream_core:frame(heartbeat)),
    State0 = rabbit_stream_core:incoming_data(ValidFrame, Init),
    %% Parsed commands must survive a frame_max change.
    State1 = rabbit_stream_core:set_frame_max(TightenedMax, State0),
    {[heartbeat], _} = rabbit_stream_core:all_commands(State1),
    ok.

prop_frame_within_limit_accepted(_Config) ->
    run_proper(
      fun() ->
              ?FORALL({FrameMax, FrameSize},
                      {range(10, 10000), range(1, 10000)},
                      begin
                          case FrameSize =< FrameMax of
                              true ->
                                  Init = rabbit_stream_core:init(#{frame_max => FrameMax}),
                                  Payload = binary:copy(<<0>>, FrameSize),
                                  Data = <<FrameSize:32, Payload/binary>>,
                                  State = rabbit_stream_core:incoming_data(Data, Init),
                                  {Cmds, _} = rabbit_stream_core:all_commands(State),
                                  length(Cmds) =:= 1 andalso
                                      element(1, hd(Cmds)) =/= frame_too_large;
                              false ->
                                  true
                          end
                      end)
      end, [], 100).

prop_frame_exceeding_limit_rejected(_Config) ->
    run_proper(
      fun() ->
              ?FORALL({FrameMax, ExcessBytes},
                      {range(10, 1000), range(1, 1000)},
                      begin
                          Init = rabbit_stream_core:init(#{frame_max => FrameMax}),
                          DeclaredSize = FrameMax + ExcessBytes,
                          Header = <<DeclaredSize:32>>,
                          State = rabbit_stream_core:incoming_data(Header, Init),
                          {Cmds, _} = rabbit_stream_core:all_commands(State),
                          Cmds =:= [{frame_too_large, DeclaredSize, FrameMax}]
                      end)
      end, [], 100).

prop_unlimited_accepts_any_size(_Config) ->
    run_proper(
      fun() ->
              ?FORALL(FrameSize,
                      range(1, 50000),
                      begin
                          Init = rabbit_stream_core:init(#{frame_max => unlimited}),
                          Payload = binary:copy(<<0>>, FrameSize),
                          Data = <<FrameSize:32, Payload/binary>>,
                          State = rabbit_stream_core:incoming_data(Data, Init),
                          {Cmds, _} = rabbit_stream_core:all_commands(State),
                          length(Cmds) =:= 1 andalso
                              element(1, hd(Cmds)) =/= frame_too_large
                      end)
      end, [], 100).

prop_boundary_exact_limit_accepted(_Config) ->
    run_proper(
      fun() ->
              ?FORALL(FrameMax,
                      range(1, 10000),
                      begin
                          Init = rabbit_stream_core:init(#{frame_max => FrameMax}),
                          Payload = binary:copy(<<0>>, FrameMax),
                          Data = <<FrameMax:32, Payload/binary>>,
                          State = rabbit_stream_core:incoming_data(Data, Init),
                          {Cmds, _} = rabbit_stream_core:all_commands(State),
                          length(Cmds) =:= 1 andalso
                              element(1, hd(Cmds)) =/= frame_too_large
                      end)
      end, [], 100).

prop_chunked_data_same_result(_Config) ->
    run_proper(
      fun() ->
              ?FORALL({FrameMax, DeclaredSize, ChunkSizes},
                      {range(100, 1000), range(50, 2000), non_empty(list(range(1, 50)))},
                      begin
                          Init = rabbit_stream_core:init(#{frame_max => FrameMax}),
                          Header = <<DeclaredSize:32>>,
                          StateWhole = rabbit_stream_core:incoming_data(Header, Init),
                          {CmdsWhole, _} = rabbit_stream_core:all_commands(StateWhole),
                          Chunks = chunk_binary(Header, ChunkSizes),
                          StateChunked = lists:foldl(
                                           fun(Chunk, S) ->
                                                   {_, S2} = rabbit_stream_core:all_commands(S),
                                                   rabbit_stream_core:incoming_data(Chunk, S2)
                                           end, Init, Chunks),
                          {CmdsChunked, _} = rabbit_stream_core:all_commands(StateChunked),
                          CmdsWhole =:= CmdsChunked
                      end)
      end, [], 100).

prop_set_frame_max_matches_init(_Config) ->
    run_proper(
      fun() ->
              ?FORALL({InitialMax, NegotiatedMax, DeclaredSize},
                      {range(100, 5000), range(50, 5000), range(1, 5000)},
                      begin
                          Header = <<DeclaredSize:32>>,
                          Direct =
                              rabbit_stream_core:init(
                                #{frame_max => NegotiatedMax}),
                          Updated =
                              rabbit_stream_core:set_frame_max(
                                NegotiatedMax,
                                rabbit_stream_core:init(
                                  #{frame_max => InitialMax})),
                          {CmdsDirect, _} =
                              rabbit_stream_core:all_commands(
                                rabbit_stream_core:incoming_data(Header,
                                                                 Direct)),
                          {CmdsViaSet, _} =
                              rabbit_stream_core:all_commands(
                                rabbit_stream_core:incoming_data(Header,
                                                                 Updated)),
                          CmdsDirect =:= CmdsViaSet
                      end)
      end, [], 100).

chunk_binary(Bin, ChunkSizes) ->
    chunk_binary(Bin, ChunkSizes, []).

chunk_binary(<<>>, _, Acc) ->
    lists:reverse(Acc);
chunk_binary(Bin, [], Acc) ->
    lists:reverse([Bin | Acc]);
chunk_binary(Bin, [Size | Rest], Acc) ->
    case Bin of
        <<Chunk:Size/binary, Rem/binary>> ->
            chunk_binary(Rem, Rest, [Chunk | Acc]);
        _ ->
            lists:reverse([Bin | Acc])
    end.
