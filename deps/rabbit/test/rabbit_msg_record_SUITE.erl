-module(rabbit_msg_record_SUITE).

-compile(export_all).

-export([
         ]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     ampq091_roundtrip,
     unsupported_091_header_is_dropped,
     message_id_ulong,
     message_id_uuid,
     message_id_binary,
     message_id_large_binary,
     message_id_large_string
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

ampq091_roundtrip(_Config) ->
    Props = #'P_basic'{content_type = <<"text/plain">>,
                       content_encoding = <<"gzip">>,
                       headers = [{<<"x-stream-offset">>, long, 99},
                                  {<<"x-string">>, longstr, <<"a string">>},
                                  {<<"x-bool">>, bool, false},
                                  {<<"x-unsignedbyte">>, unsignedbyte, 1},
                                  {<<"x-unsignedshort">>, unsignedshort, 1},
                                  {<<"x-unsignedint">>, unsignedint, 1},
                                  {<<"x-signedint">>, signedint, 1},
                                  {<<"x-timestamp">>, timestamp, 1},
                                  {<<"x-double">>, double, 1.0},
                                  {<<"x-float">>, float, 1.0},
                                  {<<"x-binary">>, binary, <<"data">>}
                                 ],
                       delivery_mode = 2,
                       priority = 99,
                       correlation_id = <<"corr">> ,
                       reply_to = <<"reply-to">>,
                       expiration = <<"1">>,
                       message_id = <<"msg-id">>,
                       timestamp = 99,
                       type = <<"45">>,
                       user_id = <<"banana">>,
                       app_id = <<"rmq">>
                       % cluster_id = <<"adf">>
                      },
    Payload = [<<"data">>],
    test_amqp091_roundtrip(Props, Payload),
    test_amqp091_roundtrip(#'P_basic'{}, Payload),
    ok.

unsupported_091_header_is_dropped(_Config) ->
    Props = #'P_basic'{
                       headers = [
                                  {<<"x-received-from">>, array, []}
                                 ]
                      },
    MsgRecord0 = rabbit_msg_record:from_amqp091(Props, <<"payload">>),
    MsgRecord = rabbit_msg_record:init(
                  iolist_to_binary(rabbit_msg_record:to_iodata(MsgRecord0))),
    % meck:unload(),
    {PropsOut, <<"payload">>} = rabbit_msg_record:to_amqp091(MsgRecord),

    ?assertMatch(#'P_basic'{headers = undefined}, PropsOut),

    ok.

message_id_ulong(_Config) ->
    Num = 9876789,
    ULong = erlang:integer_to_binary(Num),
    P = #'v1_0.properties'{message_id = {ulong, Num},
                           correlation_id = {ulong, Num}},
    D =  #'v1_0.data'{content = <<"data">>},
    Bin = [amqp10_framing:encode_bin(P),
           amqp10_framing:encode_bin(D)],
    R = rabbit_msg_record:init(iolist_to_binary(Bin)),
    {Props, _} = rabbit_msg_record:to_amqp091(R),
    ?assertMatch(#'P_basic'{message_id = ULong,
                            correlation_id = ULong,
                            headers =
                            [
                             %% ordering shouldn't matter
                             {<<"x-correlation-id-type">>, longstr, <<"ulong">>},
                             {<<"x-message-id-type">>, longstr, <<"ulong">>}
                            ]},
                 Props),
    ok.

message_id_uuid(_Config) ->
    %% fake a uuid
    UUId = erlang:md5(term_to_binary(make_ref())),
    TextUUId = rabbit_data_coercion:to_binary(rabbit_guid:to_string(UUId)),
    P = #'v1_0.properties'{message_id = {uuid, UUId},
                           correlation_id = {uuid, UUId}},
    D =  #'v1_0.data'{content = <<"data">>},
    Bin = [amqp10_framing:encode_bin(P),
           amqp10_framing:encode_bin(D)],
    R = rabbit_msg_record:init(iolist_to_binary(Bin)),
    {Props, _} = rabbit_msg_record:to_amqp091(R),
    ?assertMatch(#'P_basic'{message_id = TextUUId,
                            correlation_id = TextUUId,
                            headers =
                            [
                             %% ordering shouldn't matter
                             {<<"x-correlation-id-type">>, longstr, <<"uuid">>},
                             {<<"x-message-id-type">>, longstr, <<"uuid">>}
                            ]},
                 Props),
    ok.

message_id_binary(_Config) ->
    %% fake a uuid
    Orig = <<"asdfasdf">>,
    Text = base64:encode(Orig),
    P = #'v1_0.properties'{message_id = {binary, Orig},
                           correlation_id = {binary, Orig}},
    D =  #'v1_0.data'{content = <<"data">>},
    Bin = [amqp10_framing:encode_bin(P),
           amqp10_framing:encode_bin(D)],
    R = rabbit_msg_record:init(iolist_to_binary(Bin)),
    {Props, _} = rabbit_msg_record:to_amqp091(R),
    ?assertMatch(#'P_basic'{message_id = Text,
                            correlation_id = Text,
                            headers =
                            [
                             %% ordering shouldn't matter
                             {<<"x-correlation-id-type">>, longstr, <<"binary">>},
                             {<<"x-message-id-type">>, longstr, <<"binary">>}
                            ]},
                 Props),
    ok.

message_id_large_binary(_Config) ->
    %% cannot fit in a shortstr
    Orig = crypto:strong_rand_bytes(500),
    P = #'v1_0.properties'{message_id = {binary, Orig},
                           correlation_id = {binary, Orig}},
    D =  #'v1_0.data'{content = <<"data">>},
    Bin = [amqp10_framing:encode_bin(P),
           amqp10_framing:encode_bin(D)],
    R = rabbit_msg_record:init(iolist_to_binary(Bin)),
    {Props, _} = rabbit_msg_record:to_amqp091(R),
    ?assertMatch(#'P_basic'{message_id = undefined,
                            correlation_id = undefined,
                            headers =
                            [
                             %% ordering shouldn't matter
                             {<<"x-correlation-id">>, longstr, Orig},
                             {<<"x-message-id">>, longstr, Orig}
                            ]},
                 Props),
    ok.

message_id_large_string(_Config) ->
    %% cannot fit in a shortstr
    Orig = base64:encode(crypto:strong_rand_bytes(500)),
    P = #'v1_0.properties'{message_id = {utf8, Orig},
                           correlation_id = {utf8, Orig}},
    D =  #'v1_0.data'{content = <<"data">>},
    Bin = [amqp10_framing:encode_bin(P),
           amqp10_framing:encode_bin(D)],
    R = rabbit_msg_record:init(iolist_to_binary(Bin)),
    {Props, _} = rabbit_msg_record:to_amqp091(R),
    ?assertMatch(#'P_basic'{message_id = undefined,
                            correlation_id = undefined,
                            headers =
                            [
                             %% ordering shouldn't matter
                             {<<"x-correlation-id">>, longstr, Orig},
                             {<<"x-message-id">>, longstr, Orig}
                            ]},
                 Props),
    ok.

%% Utility

test_amqp091_roundtrip(Props, Payload) ->
    MsgRecord0 = rabbit_msg_record:from_amqp091(Props, Payload),
    MsgRecord = rabbit_msg_record:init(
                  iolist_to_binary(rabbit_msg_record:to_iodata(MsgRecord0))),
    % meck:unload(),
    {PropsOut, PayloadOut} = rabbit_msg_record:to_amqp091(MsgRecord),
    ?assertEqual(Props, PropsOut),
    ?assertEqual(iolist_to_binary(Payload),
                 iolist_to_binary(PayloadOut)),
    ok.


