-module(mc_unit_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbit/include/mc.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [shuffle], all_tests()}
    ].

all_tests() ->
    [
     amqpl_defaults,
     amqpl_compat,
     amqpl_table_x_header,
     amqpl_table_x_header_array_of_tbls,
     amqpl_death_records,
     amqpl_amqp_bin_amqpl,
     amqpl_cc_amqp_bin_amqpl,
     amqp_amqpl,
     amqp_to_amqpl_data_body,
     amqp_amqpl_amqp_bodies
    ].

%%%===================================================================
%%% Test cases
%%%===================================================================

amqpl_defaults(_Config) ->
    Props = #'P_basic'{},
    Payload = [<<"data">>],
    Content = #content{properties = Props,
                       payload_fragments_rev = Payload},
    Anns = #{exchange => <<"exch">>,
             routing_keys => [<<"apple">>]},
    Msg = mc:init(mc_amqpl, Content, Anns),

    ?assertEqual(undefined, mc:priority(Msg)),
    ?assertEqual(false, mc:is_persistent(Msg)),
    ?assertEqual(undefined, mc:timestamp(Msg)),
    ?assertEqual(undefined, mc:correlation_id(Msg)),
    ?assertEqual(undefined, mc:message_id(Msg)),
    ?assertEqual(undefined, mc:ttl(Msg)),
    ?assertEqual(undefined, mc:x_header("x-fruit", Msg)),

    ok.

amqpl_compat(_Config) ->
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
                                  {<<"a-binary">>, binary, <<"data">>},
                                  {<<"x-stream-filter">>, longstr, <<"apple">>}
                                 ],
                       delivery_mode = 1,
                       priority = 98,
                       correlation_id = <<"corr">> ,
                       reply_to = <<"reply-to">>,
                       expiration = <<"1">>,
                       message_id = <<"msg-id">>,
                       timestamp = 99,
                       type = <<"45">>,
                       user_id = <<"banana">>,
                       app_id = <<"rmq">>
                      },
    Payload = [<<"data">>],
    Content = #content{properties = Props,
                       payload_fragments_rev = Payload},

    XName= <<"exch">>,
    RoutingKey = <<"apple">>,
    {ok, Msg} = rabbit_basic:message_no_id(XName, RoutingKey, Content),

    ?assertEqual(98, mc:priority(Msg)),
    ?assertEqual(false, mc:is_persistent(Msg)),
    ?assertEqual(99000, mc:timestamp(Msg)),
    ?assertEqual({utf8, <<"corr">>}, mc:correlation_id(Msg)),
    ?assertEqual({utf8, <<"msg-id">>}, mc:message_id(Msg)),
    ?assertEqual(1, mc:ttl(Msg)),
    ?assertEqual({utf8, <<"apple">>}, mc:x_header(<<"x-stream-filter">>, Msg)),

    RoutingHeaders = mc:routing_headers(Msg, []),
    ct:pal("routing headers ~p", [RoutingHeaders]),
    ?assertMatch(#{<<"a-binary">> := <<"data">>,
                   <<"a-bool">> := false,
                   <<"a-double">> := 1.0,
                   <<"a-float">> := 1.0,
                   <<"a-signedint">> := 1,
                   <<"a-stream-offset">> := 99,
                   <<"a-string">> := <<"a string">>,
                   <<"a-timestamp">> := 1000,
                   <<"a-unsignedbyte">> := 1,
                   <<"a-unsignedint">> := 1,
                   <<"a-unsignedshort">> := 1,
                   <<"a-void">> := undefined}, RoutingHeaders),
    RoutingHeadersX = mc:routing_headers(Msg, [x_headers]),
    ?assertMatch(#{<<"a-binary">> := <<"data">>,
                   <<"a-bool">> := false,
                   <<"a-double">> := 1.0,
                   <<"a-float">> := 1.0,
                   <<"a-signedint">> := 1,
                   <<"a-stream-offset">> := 99,
                   <<"a-string">> := <<"a string">>,
                   <<"a-timestamp">> := 1000,
                   <<"a-unsignedbyte">> := 1,
                   <<"a-unsignedint">> := 1,
                   <<"a-unsignedshort">> := 1,
                   <<"a-void">> := undefined,
                   <<"x-stream-filter">> := <<"apple">>}, RoutingHeadersX),
    ok.


amqpl_table_x_header(_Config) ->
    Tbl = [{<<"type">>, longstr, <<"apple">>},
           {<<"count">>, long, 99}],

    Props = #'P_basic'{headers = [
                                  {<<"x-fruit">>, table, Tbl},
                                  {<<"fruit">>, table, Tbl}
                                 ]},
    Payload = [<<"data">>],
    Content = #content{properties = Props,
                       payload_fragments_rev = Payload},
    Anns = #{exchange => <<"exch">>,
             routing_keys => [<<"apple">>]},
    Msg = mc:init(mc_amqpl, Content, Anns),

    %% x-header values come back AMQP 1.0 ish formatted
    ?assertMatch({map,
                  [{{symbol, <<"type">>}, {utf8, <<"apple">>}},
                   {{symbol, <<"count">>}, {long, 99}}]},
                 mc:x_header(<<"x-fruit">>, Msg)),
    %% non x-headers should not show up
    % ?assertEqual(undefined, mc:x_header(<<"fruit">>, Msg)),

    ?assertMatch(#{<<"fruit">> := _,
                   <<"x-fruit">> := _},
                 mc:routing_headers(Msg, [x_headers])),

    ok.

amqpl_table_x_header_array_of_tbls(_Config) ->
    Tbl1 = [{<<"type">>, longstr, <<"apple">>},
            {<<"count">>, long, 99}],
    Tbl2 = [{<<"type">>, longstr, <<"orange">>},
            {<<"count">>, long, 45}],
    Props = #'P_basic'{headers = [
                                  {<<"x-fruit">>, array, [{table, Tbl1},
                                                          {table, Tbl2}]}
                                 ]},
    Payload = [<<"data">>],
    Content = #content{properties = Props,
                       payload_fragments_rev = Payload},
    Anns = #{exchange => <<"exch">>,
             routing_keys => [<<"apple">>]},
    Msg = mc:init(mc_amqpl, Content, Anns),
    ?assertMatch({list,
                  [{map,
                    [{{symbol, <<"type">>}, {utf8, <<"apple">>}},
                     {{symbol, <<"count">>}, {long, 99}}]},
                   {map,
                    [{{symbol, <<"type">>}, {utf8, <<"orange">>}},
                     {{symbol, <<"count">>}, {long, 45}}]}
                  ]},
                 mc:x_header(<<"x-fruit">>, Msg)),


    ok.

amqpl_death_records(_Config) ->
    Content = #content{class_id = 60,
                       properties = #'P_basic'{headers = []},
                       payload_fragments_rev = [<<"data">>]},
    Anns = #{exchange => <<"exch">>,
             routing_keys => [<<"apple">>]},
    Msg0 = mc:prepare(store, mc:init(mc_amqpl, Content, Anns)),

    Msg1 = mc:record_death(rejected, <<"q1">>, Msg0),
    ?assertEqual([<<"q1">>], mc:death_queue_names(Msg1)),
    ?assertMatch({{<<"q1">>, rejected},
                  #death{exchange = <<"exch">>,
                         routing_keys = [<<"apple">>],
                         count = 1}}, mc:last_death(Msg1)),
    ?assertEqual(false, mc:is_death_cycle(<<"q1">>, Msg1)),

    #content{properties = #'P_basic'{headers = H1}} = mc:protocol_state(Msg1),
    ?assertMatch({_, array, [_]}, header(<<"x-death">>, H1)),
    ?assertMatch({_, longstr, <<"q1">>}, header(<<"x-first-death-queue">>, H1)),
    ?assertMatch({_, longstr, <<"q1">>}, header(<<"x-last-death-queue">>, H1)),
    ?assertMatch({_, longstr, <<"exch">>}, header(<<"x-first-death-exchange">>, H1)),
    ?assertMatch({_, longstr, <<"exch">>}, header(<<"x-last-death-exchange">>, H1)),
    ?assertMatch({_, longstr, <<"rejected">>}, header(<<"x-first-death-reason">>, H1)),
    ?assertMatch({_, longstr, <<"rejected">>}, header(<<"x-last-death-reason">>, H1)),
    {_, array, [{table, T1}]} = header(<<"x-death">>, H1),
    ?assertMatch({_, long, 1}, header(<<"count">>, T1)),
    ?assertMatch({_, longstr, <<"rejected">>}, header(<<"reason">>, T1)),
    ?assertMatch({_, longstr, <<"q1">>}, header(<<"queue">>, T1)),
    ?assertMatch({_, longstr, <<"exch">>}, header(<<"exchange">>, T1)),
    ?assertMatch({_, timestamp, _}, header(<<"time">>, T1)),
    ?assertMatch({_, array, [{longstr, <<"apple">>}]}, header(<<"routing-keys">>, T1)),


    %% second dead letter, e.g. a ttl reason returning to source queue

    %% record_death uses a timestamp for death record ordering, ensure
    %% it is definitely higher than the last timestamp taken
    timer:sleep(2),
    Msg2 = mc:record_death(ttl, <<"dl">>, Msg1),

    #content{properties = #'P_basic'{headers = H2}} = mc:protocol_state(Msg2),
    {_, array, [{table, T2a}, {table, T2b}]} = header(<<"x-death">>, H2),
    ?assertMatch({_, longstr, <<"dl">>}, header(<<"queue">>, T2a)),
    ?assertMatch({_, longstr, <<"q1">>}, header(<<"queue">>, T2b)),

    ct:pal("H2 ~p", [T2a]),
    ct:pal("routing headers ~p", [mc:routing_headers(Msg2, [x_headers])]),




    ok.

header(K, H) ->
    rabbit_basic:header(K, H).

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
                                  {<<"a-binary">>, binary, <<"data">>},
                                  {<<"x-stream-filter">>, longstr, <<"apple">>}
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
                      },
    Payload = [<<"data">>],
    Content = #content{properties = Props,
                       payload_fragments_rev = Payload},
    Anns = #{exchange => <<"exch">>,
             routing_keys => [<<"apple">>]},
    Msg = mc:init(mc_amqpl, Content, Anns),

    ?assertEqual(<<"exch">>, mc:get_annotation(exchange, Msg)),
    ?assertEqual([<<"apple">>], mc:get_annotation(routing_keys, Msg)),
    ?assertEqual(98, mc:priority(Msg)),
    ?assertEqual(true, mc:is_persistent(Msg)),
    ?assertEqual(99000, mc:timestamp(Msg)),
    ?assertEqual({utf8, <<"corr">>}, mc:correlation_id(Msg)),
    ?assertEqual({utf8, <<"msg-id">>}, mc:message_id(Msg)),
    ?assertEqual(1, mc:ttl(Msg)),
    ?assertEqual({utf8, <<"apple">>}, mc:x_header(<<"x-stream-filter">>, Msg)),

    RoutingHeaders = mc:routing_headers(Msg, []),

    %% roundtrip to binary
    Msg10Pre = mc:convert(mc_amqp, Msg),
    Sections = amqp10_framing:decode_bin(
                 iolist_to_binary(amqp_serialize(Msg10Pre))),
    Msg10 = mc:init(mc_amqp, Sections, #{}),
    ?assertEqual(<<"exch">>, mc:get_annotation(exchange, Msg10)),
    ?assertEqual([<<"apple">>], mc:get_annotation(routing_keys, Msg10)),
    ?assertEqual(98, mc:priority(Msg10)),
    ?assertEqual(true, mc:is_persistent(Msg10)),
    ?assertEqual(99000, mc:timestamp(Msg10)),
    ?assertEqual({utf8, <<"corr">>}, mc:correlation_id(Msg10)),
    ?assertEqual({utf8, <<"msg-id">>}, mc:message_id(Msg10)),
    ?assertEqual(1, mc:ttl(Msg10)),
    ?assertEqual({utf8, <<"apple">>}, mc:x_header(<<"x-stream-filter">>, Msg10)),
    ?assertEqual(RoutingHeaders, mc:routing_headers(Msg10, [])),

    MsgL2 = mc:convert(mc_amqpl, Msg10),

    ?assertEqual(<<"exch">>, mc:get_annotation(exchange, MsgL2)),
    ?assertEqual([<<"apple">>], mc:get_annotation(routing_keys, MsgL2)),
    ?assertEqual(98, mc:priority(MsgL2)),
    ?assertEqual(true, mc:is_persistent(MsgL2)),
    ?assertEqual(99000, mc:timestamp(MsgL2)),
    ?assertEqual({utf8, <<"corr">>}, mc:correlation_id(MsgL2)),
    ?assertEqual({utf8, <<"msg-id">>}, mc:message_id(MsgL2)),
    ?assertEqual(1, mc:ttl(MsgL2)),
    ?assertEqual({utf8, <<"apple">>}, mc:x_header(<<"x-stream-filter">>, MsgL2)),
    ?assertEqual(RoutingHeaders, mc:routing_headers(MsgL2, [])),
    ok.

amqpl_cc_amqp_bin_amqpl(_Config) ->
    Headers = [{<<"CC">>, array, [{longstr, <<"q1">>},
                                  {longstr, <<"q2">>}]}],
    Props = #'P_basic'{headers = Headers},
    Content = #content{properties = Props,
                       payload_fragments_rev = [<<"data">>]},
    X = rabbit_misc:r(<<"/">>, exchange, <<"exch">>),
    {ok, Msg} = mc_amqpl:message(X, <<"apple">>, Content, #{}, true),

    RoutingKeys =  [<<"apple">>, <<"q1">>, <<"q2">>],
    ?assertEqual(RoutingKeys, mc:get_annotation(routing_keys, Msg)),

    Msg10Pre = mc:convert(mc_amqp, Msg),
    Sections = amqp10_framing:decode_bin(
                 iolist_to_binary(amqp_serialize(Msg10Pre))),
    Msg10 = mc:init(mc_amqp, Sections, #{}),
    ?assertEqual(RoutingKeys, mc:get_annotation(routing_keys, Msg10)),

    MsgL2 = mc:convert(mc_amqpl, Msg10),
    ?assertEqual(RoutingKeys, mc:get_annotation(routing_keys, MsgL2)),
    ?assertMatch(#content{properties = #'P_basic'{headers = Headers}},
                 mc:protocol_state(MsgL2)).

thead2(T, Value) ->
    {symbol(atom_to_binary(T)), {T, Value}}.

thead(T, Value) ->
    {utf8(atom_to_binary(T)), {T, Value}}.

amqp_amqpl(_Config) ->
    H = #'v1_0.header'{priority = {ubyte, 3},
                       ttl = {uint, 20000},
                       durable = true},
    MAC = [
           {{symbol, <<"x-stream-filter">>}, {utf8, <<"apple">>}},
          thead2(list, [utf8(<<"1">>)]),
          thead2(map, [{utf8(<<"k">>), utf8(<<"v">>)}])
          ],
    M =  #'v1_0.message_annotations'{content = MAC},
    P = #'v1_0.properties'{content_type = {symbol, <<"ctype">>},
                           content_encoding = {symbol, <<"cenc">>},
                           message_id = {utf8, <<"msg-id">>},
                           correlation_id = {utf8, <<"corr-id">>},
                           user_id = {binary, <<"user-id">>},
                           reply_to = {utf8, <<"reply-to">>},
                           group_id = {utf8, <<"group-id">>},
                           creation_time = {timestamp, 10000}
                          },
    AC = [
          thead(long, 5),
          thead(ulong, 5),
          thead(utf8, <<"a-string">>),
          thead(binary, <<"data">>),
          thead(ubyte, 1),
          thead(short, 2),
          thead(ushort, 3),
          thead(uint, 4),
          thead(int, 4),
          thead(double, 5.0),
          thead(float, 6.0),
          thead(timestamp, 7000),
          thead(byte, 128),
          thead(boolean, true),
          {utf8(<<"null">>), null}
         ],
    A =  #'v1_0.application_properties'{content = AC},
    D =  #'v1_0.data'{content = <<"data">>},

    Anns = #{exchange => <<"exch">>,
             routing_keys => [<<"apple">>]},
    Msg = mc:init(mc_amqp, [H, M, P, A, D], Anns),
    %% validate source data is serialisable
    _ = amqp_serialize(Msg),

    ?assertEqual(3, mc:priority(Msg)),
    ?assertEqual(true, mc:is_persistent(Msg)),
    ?assertEqual({utf8, <<"msg-id">>}, mc:message_id(Msg)),
    ?assertEqual({utf8, <<"corr-id">>}, mc:correlation_id(Msg)),

    MsgL = mc:convert(mc_amqpl, Msg),

    ?assertEqual(3, mc:priority(MsgL)),
    ?assertEqual(true, mc:is_persistent(MsgL)),
    ?assertEqual({utf8, <<"msg-id">>}, mc:message_id(MsgL)),
    #content{properties = #'P_basic'{headers = HL} = Props} = Content = mc:protocol_state(MsgL),

    ?assertMatch(#'P_basic'{user_id = <<"user-id">>}, Props),
    ?assertMatch(#'P_basic'{reply_to = <<"reply-to">>}, Props),
    ?assertMatch(#'P_basic'{content_type = <<"ctype">>}, Props),
    ?assertMatch(#'P_basic'{content_encoding = <<"cenc">>}, Props),
    ?assertMatch(#'P_basic'{app_id = <<"group-id">>}, Props),
    ?assertMatch(#'P_basic'{timestamp = 10}, Props),
    ?assertMatch(#'P_basic'{delivery_mode = 2}, Props),
    ?assertMatch(#'P_basic'{priority = 3}, Props),
    ?assertMatch(#'P_basic'{expiration = <<"20000">>}, Props),

    ?assertMatch({_, longstr, <<"apple">>}, header(<<"x-stream-filter">>, HL)),

    ?assertMatch({_, long, 5}, header(<<"long">>, HL)),
    ?assertMatch({_, long, 5}, header(<<"ulong">>, HL)),
    ?assertMatch({_, longstr, <<"a-string">>}, header(<<"utf8">>, HL)),
    ?assertMatch({_, binary, <<"data">>}, header(<<"binary">>, HL)),
    ?assertMatch({_, unsignedbyte, 1}, header(<<"ubyte">>, HL)),
    ?assertMatch({_, short, 2}, header(<<"short">>, HL)),
    ?assertMatch({_, unsignedshort, 3}, header(<<"ushort">>, HL)),
    ?assertMatch({_, unsignedint, 4}, header(<<"uint">>, HL)),
    ?assertMatch({_, signedint, 4}, header(<<"int">>, HL)),
    ?assertMatch({_, double, 5.0}, header(<<"double">>, HL)),
    ?assertMatch({_, float, 6.0}, header(<<"float">>, HL)),
    ?assertMatch({_, timestamp, 7}, header(<<"timestamp">>, HL)),
    ?assertMatch({_, byte, 128}, header(<<"byte">>, HL)),
    ?assertMatch({_, bool, true}, header(<<"boolean">>, HL)),
    ?assertMatch({_, void, undefined}, header(<<"null">>, HL)),

    %% validate content is serialisable
    _ = rabbit_binary_generator:build_simple_content_frames(1, Content,
                                                            1000000,
                                                            rabbit_framing_amqp_0_9_1),

    ok.

amqp_to_amqpl_data_body(_Config) ->
    Cases = [#'v1_0.data'{content = <<"helloworld">>},
             #'v1_0.data'{content = [<<"hello">>, <<"world">>]}],
    lists:foreach(
      fun(Section) ->
              Sections = case is_list(Section) of
                             true -> Section;
                             false -> [Section]
                         end,
              Mc0 = mc:init(mc_amqp, Sections, #{}),
              Mc = mc:convert(mc_amqpl, Mc0),
              #content{payload_fragments_rev = PayFragRev} = mc:protocol_state(Mc),
              PayFrag = lists:reverse(PayFragRev),
              ?assertEqual(<<"helloworld">>,
                           iolist_to_binary(PayFrag))
      end, Cases).

amqp_amqpl_amqp_bodies(_Config) ->
    Props = #'P_basic'{type = <<"amqp-1.0">>},
    Bodies = [
              #'v1_0.data'{content = <<"helo world">>},
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

         Ex = #resource{virtual_host = <<"/">>,
                        kind = exchange,
                        name = <<"ex">>},
         {ok, LegacyMsg} = mc_amqpl:message(Ex, <<"rkey">>,
                                            #content{payload_fragments_rev =
                                                     lists:reverse(EncodedPayload),
                                                     properties = Props},
                                            #{}, true),

         AmqpMsg = mc:convert(mc_amqp, LegacyMsg),
         %% drop any non body sections
         BodySections = lists:nthtail(3, mc:protocol_state(AmqpMsg)),

         AssertBody = case is_list(Payload) of
                          true ->
                              Payload;
                          false ->
                              [Payload]
                      end,
         % ct:pal("ProtoState ~p", [BodySections]),
         ?assertEqual(AssertBody, BodySections)
     end || Payload <- Bodies],
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
    [iolist_to_binary(amqp10_framing:encode_bin(X)) || X <- L];
amqp10_encode_bin(X) ->
    [iolist_to_binary(amqp10_framing:encode_bin(X))].

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

utf8(V) ->
    {utf8, V}.
symbol(V) ->
    {symbol, V}.

amqp_serialize(Msg) ->
    mc_amqp:serialize(mc:protocol_state(Msg)).
