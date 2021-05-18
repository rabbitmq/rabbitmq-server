-module(rabbit_stream_core_SUITE).

-compile(export_all).

%% Common Test callb
% -include_lib("proper/include/proper.hrl").
% -include_lib("common_test/include/ct.hrl").

-include("rabbit_stream.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [{group, tests}].

suite() ->
    [{timetrap, {seconds, 30}}].

groups() ->
    [{tests, [],
      [roundtrip, roundtrip_metadata, roundtrip_metadata_no_leader]}].

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
    test_roundtrip({publish_confirm, 42, [1, 2, 3]}),

    test_roundtrip({publish_error,
                    42,
                    ?RESPONSE_CODE_STREAM_DOES_NOT_EXIST,
                    [2, 3, 4]}),
    test_roundtrip({deliver, 53, <<"chunk">>}),
    test_roundtrip({credit, 53, 12}),
    test_roundtrip({metadata_update, <<"stream1">>,
                    ?RESPONSE_VHOST_ACCESS_FAILURE}),
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
                    {commit_offset, <<"offset_ref">>, <<"stream">>, 12}}),
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
    %% RESPONSES
    [test_roundtrip({response, 99, {Tag, 53}})
     || Tag
            <- [declare_publisher,
                delete_publisher,
                subscribe,
                unsubscribe,
                create_stream,
                delete_stream,
                open,
                close]],

    test_roundtrip({response, 99, {query_publisher_sequence, 98, 1234}}),
    test_roundtrip({response, 99, {query_offset, 1, 12}}),

    test_roundtrip({response, 99,
                    {peer_properties, 1, #{<<"k1">> => <<"v1">>}}}),

    test_roundtrip({response, 99,
                    {sasl_handshake, 1, [<<"m1">>, <<"m2">>]}}),
    test_roundtrip({response, 99,
                    {sasl_authenticate, 1, <<"challenge">>}}),
    test_roundtrip({response, 0, {tune, 10000, 12345}}),
    %  %% NB: does not write correlation id
    test_roundtrip({response, 0, {credit, 98, 200}}),
    %  %% TODO should route return a list of routed streams?
    test_roundtrip({response, 99, {route, 1, <<"stream_name">>}}),
    test_roundtrip({response, 99,
                    {partitions, 1, [<<"stream1">>, <<"stream2">>]}}),
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

test_roundtrip(Cmd) ->
    Init = rabbit_stream_core:init(undefined),
    Frame = iolist_to_binary(rabbit_stream_core:frame(Cmd)),
    {_, [Cmd]} = rabbit_stream_core:incoming_data(Frame, Init),
    ok.
