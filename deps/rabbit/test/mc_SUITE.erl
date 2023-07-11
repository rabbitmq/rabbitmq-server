-module(mc_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
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
     amqpl_amqp_bin_amqpl,
     stuff
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

stuff(_Config) ->
    MA = #'v1_0.message_annotations'{content = [{{symbol, <<"k">>}, {utf8, <<"v">>}}]},
    % Desc = {described,
    %         {utf8, <<"URL">>},
    %         {utf8, <<"https://rabbitmq.com">>}},
    MAEnc = amqp10_framing:encode(MA),
    ct:pal("~p", [MAEnc]),
    ct:pal("~p", [amqp10_framing:decode(MAEnc)]),


    % amqp10_framing:decode(Desc),



    ok.

amqpl_amqp_bin_amqpl(_Config) ->
    %% incoming amqpl converted to amqp, serialized / deserialized then converted
    %% back to amqpl.
    %% simulates a legacy message published then consumed to a stream
    Props = #'P_basic'{content_type = <<"text/plain">>,
                       content_encoding = <<"gzip">>,
                       headers = [{<<"a-stream-offset">>, long, 99},
                                  {<<"a-string">>, longstr, <<"a string">>},
                                  {<<"a-bool">>, bool, false},
                                  {<<"a-unsignedbyte">>, unsignedbyte, 1},
                                  {<<"a-unsignedshort">>, unsignedshort, 1},
                                  {<<"a-unsignedint">>, unsignedint, 1},
                                  {<<"a-signedint">>, signedint, 1},
                                  {<<"a-timestamp">>, timestamp, 1},
                                  {<<"a-double">>, double, 1.0},
                                  {<<"a-float">>, float, 1.0},
                                  {<<"a-void">>, void, undefined},
                                  {<<"a-binary">>, binary, <<"data">>}
                                 ],
                       delivery_mode = 2,
                       priority = 98,
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
    Content = #content{properties = Props,
                       payload_fragments_rev = Payload},
    Anns = #{exchange => <<"exch">>,
             routing_keys => [<<"apple">>]},
    Msg = mc:init(mc_amqpl, Content, Anns),

    ?assertEqual(98, mc:priority(Msg)),
    ?assertEqual(true, mc:is_persistent(Msg)),
    ?assertEqual(99000, mc:timestamp(Msg)),
    ?assertEqual({utf8, <<"corr">>}, mc:correlation_id(Msg)),
    ?assertEqual({utf8, <<"msg-id">>}, mc:message_id(Msg)),
    ?assertEqual(1, mc:ttl(Msg)),

    RoutingHeaders = mc:routing_headers(Msg, []),

    %% roundtrip to binary
    Msg10Pre = mc:convert(mc_amqp, Msg),
    Sections = amqp10_framing:decode_bin(
                 iolist_to_binary(mc:serialize(Msg10Pre))),
    Msg10 = mc:init(mc_amqp, Sections, #{}),
    ?assertEqual(98, mc:priority(Msg10)),
    ?assertEqual(true, mc:is_persistent(Msg10)),
    ?assertEqual(99000, mc:timestamp(Msg10)),
    ?assertEqual({utf8, <<"corr">>}, mc:correlation_id(Msg10)),
    ?assertEqual({utf8, <<"msg-id">>}, mc:message_id(Msg10)),
    ?assertEqual(1, mc:ttl(Msg10)),
    ?assertEqual(RoutingHeaders, mc:routing_headers(Msg10, [])),

    MsgL2 = mc:convert(mc_amqpl, Msg10),

    ?assertEqual(98, mc:priority(MsgL2)),
    ?assertEqual(true, mc:is_persistent(MsgL2)),
    ?assertEqual(99000, mc:timestamp(MsgL2)),
    ?assertEqual({utf8, <<"corr">>}, mc:correlation_id(MsgL2)),
    ?assertEqual({utf8, <<"msg-id">>}, mc:message_id(MsgL2)),
    ?assertEqual(1, mc:ttl(MsgL2)),
    ?assertEqual(RoutingHeaders, mc:routing_headers(MsgL2, [])),


    ok.

amqp10_non_single_data_bodies(_Config) ->
    Props = #'P_basic'{type = <<"amqp-1.0">>},
    Payloads = [
                [#'v1_0.data'{content = <<"hello">>},
                 #'v1_0.data'{content = <<"brave">>},
                 #'v1_0.data'{content = <<"new">>},
                 #'v1_0.data'{content = <<"world">>}
                ],
                #'v1_0.amqp_value'{content = {utf8, <<"hello world">>}},
                [#'v1_0.amqp_sequence'{content = [{utf8, <<"one">>},
                                                  {utf8, <<"blah">>}]},
                 #'v1_0.amqp_sequence'{content = [{utf8, <<"two">>}]}
                ]
               ],

    [begin
         EncodedPayload = amqp10_encode_bin(Payload),

         MsgRecord0 = rabbit_msg_record:from_amqp091(Props, EncodedPayload),
         MsgRecord = rabbit_msg_record:init(
                       iolist_to_binary(rabbit_msg_record:to_iodata(MsgRecord0))),
         {PropsOut, PayloadEncodedOut} = rabbit_msg_record:to_amqp091(MsgRecord),
         PayloadOut = case amqp10_framing:decode_bin(iolist_to_binary(PayloadEncodedOut)) of
                          L when length(L) =:= 1 ->
                              lists:nth(1, L);
                          L ->
                              L
                      end,

         ?assertEqual(Props, PropsOut),
         ?assertEqual(iolist_to_binary(EncodedPayload),
                      iolist_to_binary(PayloadEncodedOut)),
         ?assertEqual(Payload, PayloadOut)

     end || Payload <- Payloads],
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

reuse_amqp10_binary_chunks(_Config) ->
    Amqp10MsgAnnotations = #'v1_0.message_annotations'{content =
                                                       [{{symbol, <<"x-route">>}, {utf8, <<"dummy">>}}]},
    Amqp10MsgAnnotationsBin = amqp10_encode_bin(Amqp10MsgAnnotations),
    Amqp10Props = #'v1_0.properties'{group_id = {utf8, <<"my-group">>},
                                     group_sequence = {uint, 42}},
    Amqp10PropsBin = amqp10_encode_bin(Amqp10Props),
    Amqp10AppProps = #'v1_0.application_properties'{content = [{{utf8, <<"foo">>}, {utf8, <<"bar">>}}]},
    Amqp10AppPropsBin = amqp10_encode_bin(Amqp10AppProps),
    Amqp091Headers = [{<<"x-amqp-1.0-message-annotations">>, longstr, Amqp10MsgAnnotationsBin},
                      {<<"x-amqp-1.0-properties">>, longstr, Amqp10PropsBin},
                      {<<"x-amqp-1.0-app-properties">>, longstr, Amqp10AppPropsBin}],
    Amqp091Props = #'P_basic'{type= <<"amqp-1.0">>, headers = Amqp091Headers},
    Body = #'v1_0.amqp_value'{content = {utf8, <<"hello world">>}},
    EncodedBody = amqp10_encode_bin(Body),
    R = rabbit_msg_record:from_amqp091(Amqp091Props, EncodedBody),
    RBin = rabbit_msg_record:to_iodata(R),
    Amqp10DecodedMsg = amqp10_framing:decode_bin(iolist_to_binary(RBin)),
    [Amqp10DecodedMsgAnnotations, Amqp10DecodedProps,
     Amqp10DecodedAppProps, DecodedBody] = Amqp10DecodedMsg,
    ?assertEqual(Amqp10MsgAnnotations, Amqp10DecodedMsgAnnotations),
    ?assertEqual(Amqp10Props, Amqp10DecodedProps),
    ?assertEqual(Amqp10AppProps, Amqp10DecodedAppProps),
    ?assertEqual(Body, DecodedBody),
    ok.

amqp10_encode_bin(L) when is_list(L) ->
    iolist_to_binary([amqp10_encode_bin(X) || X <- L]);
amqp10_encode_bin(X) ->
    iolist_to_binary(amqp10_framing:encode_bin(X)).

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
