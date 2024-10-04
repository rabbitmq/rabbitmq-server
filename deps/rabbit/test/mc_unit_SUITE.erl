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
     mc_util_uuid_to_urn_roundtrip,
     amqpl_defaults,
     amqpl_compat,
     amqpl_table_x_header,
     amqpl_table_x_header_array_of_tbls,
     amqpl_death_v1_records,
     amqpl_death_v2_records,
     is_death_cycle,
     amqpl_amqp_bin_amqpl,
     amqpl_cc_amqp_bin_amqpl,
     amqp_amqpl_amqp_uuid_correlation_id,
     amqp_amqpl,
     amqp_amqpl_message_id_ulong,
     amqp_amqpl_amqp_message_id_uuid,
     amqp_amqpl_message_id_large,
     amqp_amqpl_message_id_binary,
     amqp_amqpl_unsupported_values_not_converted,
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
    Msg = mc:init(mc_amqpl, Content, annotations()),

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
    {ok, Msg00} = rabbit_basic:message_no_id(XName, RoutingKey, Content),

    %% Quorum queues set the AMQP 1.0 specific annotation delivery_count.
    %% This should be a no-op for mc_compat.
    Msg0 = mc:set_annotation(delivery_count, 1, Msg00),
    %% However, annotation x-delivery-count has a meaning for mc_compat messages.
    Msg = mc:set_annotation(<<"x-delivery-count">>, 2, Msg0),
    ?assertEqual({long, 2}, mc:x_header(<<"x-delivery-count">>, Msg)),

    ?assertEqual(98, mc:priority(Msg)),
    ?assertEqual(false, mc:is_persistent(Msg)),
    ?assertEqual(99000, mc:timestamp(Msg)),
    ?assertEqual({utf8, <<"corr">>}, mc:correlation_id(Msg)),
    ?assertEqual({utf8, <<"msg-id">>}, mc:message_id(Msg)),
    ?assertEqual(1, mc:ttl(Msg)),
    ?assertEqual({utf8, <<"apple">>}, mc:x_header(<<"x-stream-filter">>, Msg)),

    RoutingHeaders = mc:routing_headers(Msg, []),
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
    Msg = mc:init(mc_amqpl, Content, annotations()),

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
    Msg = mc:init(mc_amqpl, Content, annotations()),
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

amqpl_death_v1_records(_Config) ->
    ok = amqpl_death_records(#{?FF_MC_DEATHS_V2 => false}).

amqpl_death_v2_records(_Config) ->
    ok = amqpl_death_records(#{?FF_MC_DEATHS_V2 => true}).

amqpl_death_records(Env) ->
    Content = #content{class_id = 60,
                       properties = #'P_basic'{headers = []},
                       payload_fragments_rev = [<<"data">>]},
    Msg0 = mc:prepare(store, mc:init(mc_amqpl, Content, annotations())),

    Msg1 = mc:record_death(rejected, <<"q1">>, Msg0, Env),
    ?assertEqual([<<"q1">>], mc:death_queue_names(Msg1)),
    ?assertEqual(false, mc:is_death_cycle(<<"q1">>, Msg1)),

    #content{properties = #'P_basic'{headers = H1}} = mc:protocol_state(Msg1),
    ?assertMatch({_, array, [_]}, header(<<"x-death">>, H1)),
    ?assertMatch({_, longstr, <<"q1">>}, header(<<"x-first-death-queue">>, H1)),
    ?assertMatch({_, longstr, <<"exch">>}, header(<<"x-first-death-exchange">>, H1)),
    ?assertMatch({_, longstr, <<"rejected">>}, header(<<"x-first-death-reason">>, H1)),
    ?assertMatch({_, longstr, <<"q1">>}, header(<<"x-last-death-queue">>, H1)),
    ?assertMatch({_, longstr, <<"exch">>}, header(<<"x-last-death-exchange">>, H1)),
    ?assertMatch({_, longstr, <<"rejected">>}, header(<<"x-last-death-reason">>, H1)),
    {_, array, [{table, T1}]} = header(<<"x-death">>, H1),
    ?assertMatch({_, long, 1}, header(<<"count">>, T1)),
    ?assertMatch({_, longstr, <<"rejected">>}, header(<<"reason">>, T1)),
    ?assertMatch({_, longstr, <<"q1">>}, header(<<"queue">>, T1)),
    ?assertMatch({_, longstr, <<"exch">>}, header(<<"exchange">>, T1)),
    ?assertMatch({_, timestamp, _}, header(<<"time">>, T1)),
    ?assertMatch({_, array, [{longstr, <<"apple">>}]}, header(<<"routing-keys">>, T1)),


    %% second dead letter, e.g. an expired reason returning to source queue

    %% record_death uses a timestamp for death record ordering, ensure
    %% it is definitely higher than the last timestamp taken
    timer:sleep(2),
    Msg2 = mc:record_death(expired, <<"dl">>, Msg1, Env),

    #content{properties = #'P_basic'{headers = H2}} = mc:protocol_state(Msg2),
    {_, array, [{table, T2a}, {table, T2b}]} = header(<<"x-death">>, H2),
    ?assertMatch({_, longstr, <<"dl">>}, header(<<"queue">>, T2a)),
    ?assertMatch({_, longstr, <<"q1">>}, header(<<"queue">>, T2b)),
    ok.

is_death_cycle(_Config) ->
    Content = #content{class_id = 60,
                       properties = #'P_basic'{headers = []},
                       payload_fragments_rev = [<<"data">>]},
    Msg0 = mc:prepare(store, mc:init(mc_amqpl, Content, annotations())),

    %% Test the followig topology:
    %% Q1 --rejected--> Q2 --expired--> Q3 --expired-->
    %% Q1 --rejected--> Q2 --expired--> Q3

    Msg1 = mc:record_death(rejected, <<"q1">>, Msg0, #{}),
    ?assertNot(mc:is_death_cycle(<<"q1">>, Msg1),
               "A queue that dead letters to itself due to rejected is not considered a cycle."),
    ?assertNot(mc:is_death_cycle(<<"q2">>, Msg1)),
    ?assertNot(mc:is_death_cycle(<<"q3">>, Msg1)),

    Msg2 = mc:record_death(expired, <<"q2">>, Msg1, #{}),
    ?assertNot(mc:is_death_cycle(<<"q1">>, Msg2)),
    ?assert(mc:is_death_cycle(<<"q2">>, Msg2),
            "A queue that dead letters to itself due to expired is considered a cycle."),
    ?assertNot(mc:is_death_cycle(<<"q3">>, Msg2)),

    Msg3 = mc:record_death(expired, <<"q3">>, Msg2, #{}),
    ?assertNot(mc:is_death_cycle(<<"q1">>, Msg3)),
    ?assert(mc:is_death_cycle(<<"q2">>, Msg3)),
    ?assert(mc:is_death_cycle(<<"q3">>, Msg3)),

    Msg4 = mc:record_death(rejected, <<"q1">>, Msg3, #{}),
    ?assertNot(mc:is_death_cycle(<<"q1">>, Msg4)),
    ?assertNot(mc:is_death_cycle(<<"q2">>, Msg4)),
    ?assertNot(mc:is_death_cycle(<<"q3">>, Msg4)),

    Msg5 = mc:record_death(expired, <<"q2">>, Msg4, #{}),
    ?assertNot(mc:is_death_cycle(<<"q1">>, Msg5)),
    ?assert(mc:is_death_cycle(<<"q2">>, Msg5)),
    ?assertNot(mc:is_death_cycle(<<"q3">>, Msg5)),

    DeathQsOrderedByRecency = [<<"q2">>, <<"q1">>, <<"q3">>],
    ?assertEqual(DeathQsOrderedByRecency, mc:death_queue_names(Msg5)),

    #content{properties = #'P_basic'{headers = H}} = mc:protocol_state(Msg5),
    ?assertMatch({_, longstr, <<"q1">>}, header(<<"x-first-death-queue">>, H)),
    ?assertMatch({_, longstr, <<"rejected">>}, header(<<"x-first-death-reason">>, H)),
    ?assertMatch({_, longstr, <<"q2">>}, header(<<"x-last-death-queue">>, H)),
    ?assertMatch({_, longstr, <<"expired">>}, header(<<"x-last-death-reason">>, H)),

    %% We expect the array to be ordered by recency.
    {_, array, [{table, T1}, {table, T2}, {table, T3}]} = header(<<"x-death">>, H),

    ?assertMatch({_, longstr, <<"q2">>}, header(<<"queue">>, T1)),
    ?assertMatch({_, longstr, <<"expired">>}, header(<<"reason">>, T1)),
    ?assertMatch({_, long, 2}, header(<<"count">>, T1)),

    ?assertMatch({_, longstr, <<"q1">>}, header(<<"queue">>, T2)),
    ?assertMatch({_, longstr, <<"rejected">>}, header(<<"reason">>, T2)),
    ?assertMatch({_, long, 2}, header(<<"count">>, T2)),

    ?assertMatch({_, longstr, <<"q3">>}, header(<<"queue">>, T3)),
    ?assertMatch({_, longstr, <<"expired">>}, header(<<"reason">>, T3)),
    ?assertMatch({_, long, 1}, header(<<"count">>, T3)).

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
                                  {<<"a-array">>, array, [{long, 1}, {long, 2}]},
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
    Content = #content{properties = Props,
                       payload_fragments_rev = [<<"data">>]},
    Msg = mc:init(mc_amqpl, Content, annotations()),

    ?assertEqual(<<"exch">>, mc:exchange(Msg)),
    ?assertEqual([<<"apple">>], mc:routing_keys(Msg)),
    ?assertEqual(98, mc:priority(Msg)),
    ?assertEqual(true, mc:is_persistent(Msg)),
    ?assertEqual(99000, mc:timestamp(Msg)),
    ?assertEqual({utf8, <<"corr">>}, mc:correlation_id(Msg)),
    ?assertEqual({utf8, <<"msg-id">>}, mc:message_id(Msg)),
    ?assertEqual(1, mc:ttl(Msg)),
    ?assertEqual({utf8, <<"apple">>}, mc:x_header(<<"x-stream-filter">>, Msg)),
    ?assert(is_integer(mc:get_annotation(rts, Msg))),

    %% array type non x-headers cannot be converted into amqp
    RoutingHeaders = maps:remove(<<"a-array">>, mc:routing_headers(Msg, [])),

    %% roundtrip to binary
    Msg10Pre = mc:convert(mc_amqp, Msg),
    Payload = iolist_to_binary(mc:protocol_state(Msg10Pre)),
    Msg10 = mc:init(mc_amqp, Payload, #{}),
    ?assertEqual(<<"exch">>, mc:exchange(Msg10)),
    ?assertEqual([<<"apple">>], mc:routing_keys(Msg10)),
    ?assertEqual(98, mc:priority(Msg10)),
    ?assertEqual(true, mc:is_persistent(Msg10)),
    ?assertEqual(99000, mc:timestamp(Msg10)),
    ?assertEqual({utf8, <<"corr">>}, mc:correlation_id(Msg10)),
    ?assertEqual({utf8, <<"msg-id">>}, mc:message_id(Msg10)),
    ?assertEqual(1, mc:ttl(Msg10)),
    ?assertEqual({utf8, <<"apple">>}, mc:x_header(<<"x-stream-filter">>, Msg10)),
    %% at this point the type is now present as a message annotation
    ?assertEqual({utf8, <<"45">>}, mc:x_header(<<"x-basic-type">>, Msg10)),
    ?assertEqual(RoutingHeaders, mc:routing_headers(Msg10, [])),
    ?assert(is_integer(mc:get_annotation(rts, Msg10))),

    Sections = amqp10_framing:decode_bin(Payload),
    [
     #'v1_0.header'{} = Hdr10,
     #'v1_0.message_annotations'{},
     #'v1_0.properties'{} = Props10,
     #'v1_0.application_properties'{content = AP10}
     | _] = Sections,

    ?assertMatch(#'v1_0.header'{durable = true,
                                ttl = {uint, 1},
                                priority = {ubyte, 98}},
                 Hdr10),
    ?assertMatch(#'v1_0.properties'{content_encoding = {symbol, <<"gzip">>},
                                    content_type = {symbol, <<"text/plain">>},
                                    reply_to = {utf8, <<"reply-to">>},
                                    creation_time = {timestamp, 99000},
                                    user_id = {binary, <<"banana">>},
                                    group_id = {utf8, <<"rmq">>}
                                   },
                 Props10),

    Get = fun(K, AP) -> amqp_map_get(utf8(K), AP) end,


    ?assertEqual({long, 99}, Get(<<"a-stream-offset">>, AP10)),
    ?assertEqual({utf8, <<"a string">>}, Get(<<"a-string">>, AP10)),
    ?assertEqual(false, Get(<<"a-bool">>, AP10)),
    ?assertEqual({ubyte, 1}, Get(<<"a-unsignedbyte">>, AP10)),
    ?assertEqual({ushort, 1}, Get(<<"a-unsignedshort">>, AP10)),
    ?assertEqual({uint, 1}, Get(<<"a-unsignedint">>, AP10)),
    ?assertEqual({int, 1}, Get(<<"a-signedint">>, AP10)),
    ?assertEqual({timestamp, 1000}, Get(<<"a-timestamp">>, AP10)),
    ?assertEqual({double, 1.0}, Get(<<"a-double">>, AP10)),
    ?assertEqual({float, 1.0}, Get(<<"a-float">>, AP10)),
    ?assertEqual(undefined, Get(<<"a-void">>, AP10)),
    ?assertEqual({binary, <<"data">>}, Get(<<"a-binary">>, AP10)),
    %% x-headers do not go into app props
    ?assertEqual(undefined, Get(<<"x-stream-filter">>, AP10)),
    %% arrays are not converted
    ?assertEqual(undefined, Get(<<"a-array">>, AP10)),
    %% assert properties

    MsgL2 = mc:convert(mc_amqpl, Msg10),

    ?assertEqual(<<"exch">>, mc:exchange(MsgL2)),
    ?assertEqual([<<"apple">>], mc:routing_keys(MsgL2)),
    ?assertEqual(98, mc:priority(MsgL2)),
    ?assertEqual(true, mc:is_persistent(MsgL2)),
    ?assertEqual(99000, mc:timestamp(MsgL2)),
    ?assertEqual({utf8, <<"corr">>}, mc:correlation_id(MsgL2)),
    ?assertEqual({utf8, <<"msg-id">>}, mc:message_id(MsgL2)),
    ?assertEqual(1, mc:ttl(MsgL2)),
    ?assertEqual({utf8, <<"apple">>}, mc:x_header(<<"x-stream-filter">>, MsgL2)),
    ?assertEqual(RoutingHeaders, mc:routing_headers(MsgL2, [])),
    ?assert(is_integer(mc:get_annotation(rts, MsgL2))),
    ok.

amqpl_cc_amqp_bin_amqpl(_Config) ->
    Headers = [{<<"CC">>, array, [{longstr, <<"q1">>},
                                  {longstr, <<"q2">>}]}],
    Props = #'P_basic'{headers = Headers},
    Content = #content{properties = Props,
                       payload_fragments_rev = [<<"data">>]},
    X = rabbit_misc:r(<<"/">>, exchange, <<"exch">>),
    {ok, Msg} = mc_amqpl:message(X, <<"apple">>, Content, #{}),

    RoutingKeys =  [<<"apple">>, <<"q1">>, <<"q2">>],
    ?assertEqual(RoutingKeys, mc:routing_keys(Msg)),

    Msg10Pre = mc:convert(mc_amqp, Msg),
    Sections = iolist_to_binary(mc:protocol_state(Msg10Pre)),
    Msg10 = mc:init(mc_amqp, Sections, #{}),
    ?assertEqual(RoutingKeys, mc:routing_keys(Msg10)),

    MsgL2 = mc:convert(mc_amqpl, Msg10),
    ?assertEqual(RoutingKeys, mc:routing_keys(MsgL2)),
    ?assertMatch(#content{properties = #'P_basic'{headers = Headers}},
                 mc:protocol_state(MsgL2)).

thead2(T, Value) ->
    {symbol(atom_to_binary(T)), {T, Value}}.

thead2(K, T, Value) ->
    {symbol(atom_to_binary(K)), {T, Value}}.

thead(T, Value) ->
    {utf8(atom_to_binary(T)), {T, Value}}.

mc_util_uuid_to_urn_roundtrip(_Config) ->
    %% roundtrip uuid test
    UUID = <<88,184,103,176,129,81,31,86,27,212,115,34,152,7,253,96>>,
    S = mc_util:uuid_to_urn_string(UUID),
    ?assertEqual(<<"urn:uuid:58b867b0-8151-1f56-1bd4-73229807fd60">>, S),
    ?assertEqual({ok, UUID}, mc_util:urn_string_to_uuid(S)),
    ok.

do_n(0, _) ->
    ok;
do_n(N, Fun) ->
    Fun(),
    do_n(N -1, Fun).

amqp_amqpl_unsupported_values_not_converted(_Config) ->
    LongKey = binary:copy(<<"a">>, 256),
    UTF8Key = <<"I am a 🐰"/utf8>>,
    APC = [
           {{utf8, <<"area">>}, {utf8, <<"East Sussex">>}},
           {{utf8, LongKey}, {utf8, <<"apple">>}},
           {{utf8, UTF8Key}, {utf8, <<"dog">>}}
          ],
    AP =  #'v1_0.application_properties'{content = APC},

    %% invalid utf8
    UserId = <<0, "banana"/utf8>>,
    ?assertEqual(false, mc_util:is_valid_shortstr(UserId)),

    P = #'v1_0.properties'{user_id = {binary, UserId}},
    D =  #'v1_0.data'{content = <<"data">>},
    Payload = serialize_sections([P, AP, D]),

    Msg = mc:init(mc_amqp, Payload, annotations()),
    MsgL = mc:convert(mc_amqpl, Msg),
    #content{properties = #'P_basic'{user_id = undefined,
                                     headers = HL}} = mc:protocol_state(MsgL),
    ?assertMatch({_, longstr, <<"East Sussex">>}, header(<<"area">>, HL)),
    ?assertMatch(undefined, header(LongKey, HL)),
    %% RabbitMQ does not validate that keys are ascii as per spec
    %% that's ok after all who really cares?
    ok.

amqp_amqpl_amqp_uuid_correlation_id(_Config) ->
    %% ensure uuid correlation ids are correctly roundtripped via urn formatting
    UUID = crypto:strong_rand_bytes(16),

    P = #'v1_0.properties'{correlation_id = {uuid, UUID},
                           message_id = {uuid, UUID}},
    D =  #'v1_0.data'{content = <<"data">>},
    BareMsgIn = serialize_sections([P, D]),

    Msg = mc:init(mc_amqp, BareMsgIn, annotations()),
    MsgL = mc:convert(mc_amqpl, Msg),
    MsgOut = mc:convert(mc_amqp, MsgL),

    [_HeaderSect, _MessageAnnotationsSect | BareMsgIoList] = mc:protocol_state(MsgOut),
    BareMsgOut = iolist_to_binary(BareMsgIoList),
    ?assertEqual(BareMsgIn, BareMsgOut).

amqp_amqpl(_Config) ->
    H = #'v1_0.header'{priority = {ubyte, 3},
                       ttl = {uint, 20000},
                       durable = true},
    MAC = [
           {{symbol, <<"x-stream-filter">>}, {utf8, <<"apple">>}},
           thead2('x-list', list, [utf8(<<"l">>)]),
           thead2('x-map', map, [{utf8(<<"k">>), utf8(<<"v">>)}])
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
          thead(symbol, <<"symbol">>),
          thead(ubyte, 255),
          thead(short, 2),
          thead(ushort, 3),
          thead(uint, 4),
          thead(int, 4),
          thead(double, 5.0),
          thead(float, 6.0),
          thead(timestamp, 7000),
          thead(byte, -128),
          thead(boolean, true),
          {{utf8, <<"boolean2">>}, false},
          {utf8(<<"null">>), null}
         ],
    A =  #'v1_0.application_properties'{content = AC},
    D =  #'v1_0.data'{content = <<"data">>},

    Payload = serialize_sections([H, M, P, A, D]),
    Msg = mc:init(mc_amqp, Payload, annotations()),
    %% validate source data is serialisable
    _ = mc:protocol_state(Msg),

    ?assertEqual(3, mc:priority(Msg)),
    ?assertEqual(true, mc:is_persistent(Msg)),
    ?assertEqual({utf8, <<"msg-id">>}, mc:message_id(Msg)),
    ?assertEqual({utf8, <<"corr-id">>}, mc:correlation_id(Msg)),

    MsgL = mc:convert(mc_amqpl, Msg),

    ?assertEqual(3, mc:priority(MsgL)),
    ?assertEqual(true, mc:is_persistent(MsgL)),
    ?assertEqual({utf8, <<"msg-id">>}, mc:message_id(MsgL)),
    #content{properties = #'P_basic'{headers = HL} = Props} = Content =
        mc:protocol_state(MsgL),

    %% the user id is valid utf8 shortstr
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
    ?assertMatch({_ ,array, [{longstr,<<"l">>}]}, header(<<"x-list">>, HL)),
    ?assertMatch({_, table, [{<<"k">>,longstr,<<"v">>}]}, header(<<"x-map">>, HL)),

    ?assertMatch({_, long, 5}, header(<<"long">>, HL)),
    ?assertMatch({_, long, 5}, header(<<"ulong">>, HL)),
    ?assertMatch({_, longstr, <<"a-string">>}, header(<<"utf8">>, HL)),
    ?assertMatch({_, longstr, <<"data">>}, header(<<"binary">>, HL)),
    ?assertMatch({_, longstr, <<"symbol">>}, header(<<"symbol">>, HL)),
    ?assertMatch({_, unsignedbyte, 255}, header(<<"ubyte">>, HL)),
    ?assertMatch({_, short, 2}, header(<<"short">>, HL)),
    ?assertMatch({_, unsignedshort, 3}, header(<<"ushort">>, HL)),
    ?assertMatch({_, unsignedint, 4}, header(<<"uint">>, HL)),
    ?assertMatch({_, signedint, 4}, header(<<"int">>, HL)),
    ?assertMatch({_, double, 5.0}, header(<<"double">>, HL)),
    ?assertMatch({_, float, 6.0}, header(<<"float">>, HL)),
    ?assertMatch({_, timestamp, 7}, header(<<"timestamp">>, HL)),
    ?assertMatch({_, byte, -128}, header(<<"byte">>, HL)),
    ?assertMatch({_, bool, true}, header(<<"boolean">>, HL)),
    ?assertMatch({_, bool, false}, header(<<"boolean2">>, HL)),
    ?assertMatch({_, void, undefined}, header(<<"null">>, HL)),

    %% validate content is serialisable
    _ = rabbit_binary_generator:build_simple_content_frames(1, Content,
                                                            1000000,
                                                            rabbit_framing_amqp_0_9_1),

    ok.

amqp_amqpl_message_id_ulong(_Config) ->
    Num = 9876789,
    ULong = erlang:integer_to_binary(Num),
    P = #'v1_0.properties'{message_id = {ulong, Num},
                           correlation_id = {ulong, Num}},
    D =  #'v1_0.data'{content = <<"data">>},
    Payload = serialize_sections([P, D]),
    Msg = mc:init(mc_amqp, Payload, annotations()),
    MsgL = mc:convert(mc_amqpl, Msg),
    ?assertEqual({utf8, ULong}, mc:message_id(MsgL)),
    ?assertEqual({utf8, ULong}, mc:correlation_id(MsgL)),
    #content{properties = #'P_basic'{} = Props} = mc:protocol_state(MsgL),
    ?assertMatch(#'P_basic'{message_id = ULong,
                            correlation_id = ULong}, Props),
    %% NB we can't practically roundtrip ulong correlation ids
    ok.

amqp_amqpl_amqp_message_id_uuid(_Config) ->
    %% uuid message-ids are roundtripped using a urn uuid format
    UUId = crypto:strong_rand_bytes(16),
    Urn = mc_util:uuid_to_urn_string(UUId),
    P = #'v1_0.properties'{message_id = {uuid, UUId},
                           correlation_id = {uuid, UUId}},
    D =  #'v1_0.data'{content = <<"data">>},
    BareMsgIn = serialize_sections([P, D]),
    Msg = mc:init(mc_amqp, BareMsgIn, annotations()),
    MsgL = mc:convert(mc_amqpl, Msg),
    ?assertEqual({utf8, Urn}, mc:message_id(MsgL)),
    ?assertEqual({utf8, Urn}, mc:correlation_id(MsgL)),
    #content{properties = #'P_basic'{} = Props} = mc:protocol_state(MsgL),
    ?assertMatch(#'P_basic'{message_id = Urn,
                            correlation_id = Urn}, Props),
    %% check roundtrip back
    Msg2 = mc:convert(mc_amqp, MsgL),
    [_HeaderSect, _MessageAnnotationsSect | BareMsgIoList] = mc:protocol_state(Msg2),
    BareMsgOut = iolist_to_binary(BareMsgIoList),
    ?assertEqual(BareMsgIn, BareMsgOut).

amqp_amqpl_message_id_large(_Config) ->
    Orig = binary:copy(<<"hi">>, 256),
    P = #'v1_0.properties'{message_id = {utf8, Orig},
                           correlation_id = {utf8, Orig}},
    D =  #'v1_0.data'{content = <<"data">>},
    Payload = serialize_sections([P, D]),
    Msg = mc:init(mc_amqp, Payload, annotations()),
    MsgL = mc:convert(mc_amqpl, Msg),
    ?assertEqual(undefined, mc:message_id(MsgL)),
    ?assertEqual(undefined, mc:correlation_id(MsgL)),
    #content{properties = #'P_basic'{headers = Hdrs}} = mc:protocol_state(MsgL),
    ?assertMatch({_, longstr, Orig}, header(<<"x-message-id">>, Hdrs)),
    ?assertMatch({_, longstr, Orig}, header(<<"x-correlation-id">>, Hdrs)),
    ok.

amqp_amqpl_message_id_binary(_Config) ->
    Orig = crypto:strong_rand_bytes(128),
    P = #'v1_0.properties'{message_id = {binary, Orig},
                           correlation_id = {binary, Orig}},
    D =  #'v1_0.data'{content = <<"data">>},
    Payload = serialize_sections([P, D]),
    Msg = mc:init(mc_amqp, Payload, annotations()),
    MsgL = mc:convert(mc_amqpl, Msg),
    ?assertEqual(undefined, mc:message_id(MsgL)),
    ?assertEqual(undefined, mc:correlation_id(MsgL)),
    #content{properties = #'P_basic'{headers = Hdrs}} = mc:protocol_state(MsgL),
    ?assertMatch({_, binary, Orig}, header(<<"x-message-id">>, Hdrs)),
    ?assertMatch({_, binary, Orig}, header(<<"x-correlation-id">>, Hdrs)),
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
              Payload = serialize_sections(Sections),
              Mc0 = mc:init(mc_amqp, Payload, #{}),
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
         EncodedBody = amqp10_encode_bin(Body),
         Ex = #resource{virtual_host = <<"/">>,
                        kind = exchange,
                        name = <<"ex">>},
         {ok, LegacyMsg} = mc_amqpl:message(Ex,
                                            <<"rkey">>,
                                            #content{payload_fragments_rev =
                                                     lists:reverse(EncodedBody),
                                                     properties = Props},
                                            #{}),
         AmqpMsg = mc:convert(mc_amqp, LegacyMsg),
         %% drop any non body sections
         [_HeaderSect, _MessageAnnotationsSect | BodySectionsIoList] = mc:protocol_state(AmqpMsg),
         BodySectionsBin = iolist_to_binary(BodySectionsIoList),
         BodySections = amqp10_framing:decode_bin(BodySectionsBin),
         ExpectedBodySections = case is_list(Body) of
                                    true -> Body;
                                    false -> [Body]
                                end,
         ?assertEqual(ExpectedBodySections, BodySections)
     end || Body <- Bodies],
    ok.

%% Utility

amqp10_encode_bin(L) when is_list(L) ->
    [iolist_to_binary(amqp10_framing:encode_bin(X)) || X <- L];
amqp10_encode_bin(X) ->
    amqp10_encode_bin([X]).

serialize_sections(Sections) ->
    iolist_to_binary([amqp10_framing:encode_bin(S) || S <- Sections]).

utf8(V) ->
    {utf8, V}.

symbol(V) ->
    {symbol, V}.

amqp_map_get(_K, []) ->
    undefined;
amqp_map_get(K, Tuples) ->
    case lists:keyfind(K, 1, Tuples) of
        false ->
            undefined;
        {_, V}  ->
            V
    end.

annotations() ->
    #{?ANN_EXCHANGE => <<"exch">>,
      ?ANN_ROUTING_KEYS => [<<"apple">>]}.
