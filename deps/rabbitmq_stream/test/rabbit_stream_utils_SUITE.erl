-module(rabbit_stream_utils_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").

-import(rabbit_stream_utils,
        [validate_super_stream_max_partitions/2,
         write_messages/8]).

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [{group, tests}].

suite() ->
    [{timetrap, {seconds, 30}}].

groups() ->
    [{tests, [],
      [sort_partitions,
       filter_spec,
       filter_defined,
       test_validate_max_super_stream_partitions,
       super_stream_partition_helpers,
       write_messages_sub_batch_within_limit_accepted,
       write_messages_sub_batch_at_limit_accepted,
       write_messages_sub_batch_over_limit_rejected,
       write_messages_sub_batch_zero_message_count_rejected,
       write_messages_sub_batch_message_count_exceeds_uncompressed_size_rejected,
       write_messages_sub_batch_unknown_compression_type_rejected,
       write_messages_sub_batch_empty_batch_with_compression_rejected,
       write_messages_sub_batch_high_compression_ratio_accepted,
       write_messages_fail_fast_after_first_invalid_sub_batch,
       write_messages_returns_written_count]}].

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

sort_partitions(_Config) ->
    [] = rabbit_stream_utils:sort_partitions([]),
    ?assertEqual([<<"a">>, <<"b">>, <<"c">>],
                 [S
                  || #binding{destination = #resource{name = S}}
                         <- rabbit_stream_utils:sort_partitions([binding(<<"c">>,
                                                                         2),
                                                                 binding(<<"b">>,
                                                                         1),
                                                                 binding(<<"a">>,
                                                                         0)])]),
    ?assertEqual([<<"a">>, <<"c">>, <<"no-order-field">>],
                 [S
                  || #binding{destination = #resource{name = S}}
                         <- rabbit_stream_utils:sort_partitions([binding(<<"c">>,
                                                                         10),
                                                                 binding(<<"no-order-field">>),
                                                                 binding(<<"a">>,
                                                                         0)])]),
    ok.

filter_defined(_) ->
    [?assertEqual(Expected, rabbit_stream_utils:filter_defined(Properties))
     || {Properties, Expected} <- [
        {#{<<"filter.1">> => <<"">>}, true},
        {#{<<"filter.1">> => <<"">>,
           <<"sac">> => <<"false">>}, true},
        {#{<<"foo">> => <<"bar">>}, false},
        {#{}, false},
        {undefined, false}]].

filter_spec(_) ->
    [begin
         FilterSpec = rabbit_stream_utils:filter_spec(Properties),
         ?assert(maps:is_key(filter_spec, FilterSpec)),
         #{filter_spec := #{filters := Filters, match_unfiltered := MatchUnfiltered}} = FilterSpec,
         ?assertEqual(lists:sort(ExpectedFilters), lists:sort(Filters)),
         ?assertEqual(ExpectedMatchUnfiltered, MatchUnfiltered)
     end || {Properties, ExpectedFilters, ExpectedMatchUnfiltered} <-
            [{#{<<"filter.1">> => <<"apple">>,
                <<"filter.2">> => <<"banana">>,
                <<"sac">> => true}, [<<"apple">>, <<"banana">>], false},
             {#{<<"filter.1">> => <<"apple">>}, [<<"apple">>], false},
             {#{<<"filter.1">> => <<"apple">>,
                <<"match-unfiltered">> => <<"true">>}, [<<"apple">>], true}
            ]],
    #{} = rabbit_stream_utils:filter_spec(#{}),
    #{} = rabbit_stream_utils:filter_spec(#{<<"sac">> => true}),
    ok.

test_validate_max_super_stream_partitions(_) ->
    %% infinity means no limit applies
    ?assertEqual(true, validate_super_stream_max_partitions([], infinity)),
    ?assertEqual(true, validate_super_stream_max_partitions([a, b, c], infinity)),
    ?assertEqual(true, validate_super_stream_max_partitions(0, infinity)),
    ?assertEqual(true, validate_super_stream_max_partitions(1000, infinity)),
    %% max = 0: only an empty list and integer 0 are valid
    ?assertEqual(true, validate_super_stream_max_partitions([], 0)),
    ?assertEqual(false, validate_super_stream_max_partitions([a], 0)),
    ?assertEqual(true, validate_super_stream_max_partitions(0, 0)),
    ?assertEqual(false, validate_super_stream_max_partitions(1, 0)),
    %% exactly at the limit is valid
    ?assertEqual(true, validate_super_stream_max_partitions([a, b, c], 3)),
    ?assertEqual(true, validate_super_stream_max_partitions(3, 3)),
    %% one over the limit is invalid
    ?assertEqual(false, validate_super_stream_max_partitions([a, b, c, d], 3)),
    ?assertEqual(false, validate_super_stream_max_partitions(4, 3)),
    ok.

super_stream_partition_helpers(_) ->
    %% streams_from_partitions/2
    ?assertEqual([],
                 rabbit_stream_utils:streams_from_partitions(<<"invoices">>, 0)),
    ?assertEqual([<<"invoices-0">>],
                 rabbit_stream_utils:streams_from_partitions(<<"invoices">>, 1)),
    ?assertEqual([<<"invoices-0">>, <<"invoices-1">>, <<"invoices-2">>],
                 rabbit_stream_utils:streams_from_partitions(<<"invoices">>, 3)),

    %% streams_from_binding_keys/2
    ?assertEqual([],
                 rabbit_stream_utils:streams_from_binding_keys(<<"invoices">>, [])),
    ?assertEqual([<<"invoices-amer">>],
                 rabbit_stream_utils:streams_from_binding_keys(<<"invoices">>,
                                                               [<<"amer">>])),
    ?assertEqual([<<"invoices-amer">>, <<"invoices-emea">>, <<"invoices-apac">>],
                 rabbit_stream_utils:streams_from_binding_keys(<<"invoices">>,
                                                               [<<"amer">>,
                                                                <<"emea">>,
                                                                <<"apac">>])),

    %% routing_keys/1
    ?assertEqual([], rabbit_stream_utils:routing_keys(0)),
    ?assertEqual([<<"0">>], rabbit_stream_utils:routing_keys(1)),
    ?assertEqual([<<"0">>, <<"1">>, <<"2">>],
                 rabbit_stream_utils:routing_keys(3)),

    %% binding_keys/1
    ?assertEqual([<<"amer">>],
                 rabbit_stream_utils:binding_keys(<<"amer">>)),
    ?assertEqual([<<"amer">>, <<"emea">>, <<"apac">>],
                 rabbit_stream_utils:binding_keys(<<"amer,emea,apac">>)),
    ?assertEqual([<<"amer">>, <<"emea">>, <<"apac">>],
                 rabbit_stream_utils:binding_keys(<<"amer, emea, apac">>)),
    ?assertEqual([],
                 rabbit_stream_utils:binding_keys(<<"">>)),
    ?assertEqual([],
                 rabbit_stream_utils:binding_keys(<<"  ">>)),

    ok.

%%%===================================================================
%%% write_messages/8 - sub-entry batch validation
%%%===================================================================

%% write_messages/8 casts to osiris via gen_batch_server:cast/2, which is a
%% plain message send: using self() as the "cluster leader" lets these tests
%% assert on writes without a real osiris process.

write_messages_sub_batch_within_limit_accepted(_Config) ->
    Max = 1000,
    Batch = <<"compressed-bytes">>,
    Entry = sub_batch(1, 1, 2, 100, Batch),
    ?assertEqual({1, []}, write_messages(?VERSION_1, self(), undefined, 7, 3,
                                         Entry, Max, <<"s1">>)),
    ?assertMatch({'$gen_cast', {write, _, undefined, {7, 3, 1},
                                {batch, 2, 1, 100, Batch}}},
                 flush_one()),
    ok.

write_messages_sub_batch_at_limit_accepted(_Config) ->
    Max = 100,
    Entry = sub_batch(1, 1, 1, Max, <<"x">>),
    ?assertEqual({1, []}, write_messages(?VERSION_1, self(), undefined, 7, 1,
                                         Entry, Max, <<"s1">>)),
    ?assertMatch({'$gen_cast', _}, flush_one()),
    ok.

write_messages_sub_batch_over_limit_rejected(_Config) ->
    Max = 100,
    Entry = sub_batch(42, 1, 1, Max + 1, <<"x">>),
    ?assertEqual({0, [42]}, write_messages(?VERSION_1, self(), undefined, 7, 1,
                                           Entry, Max, <<"s1">>)),
    ?assertEqual(no_message, flush_one()),
    ok.

write_messages_sub_batch_zero_message_count_rejected(_Config) ->
    Max = 1000,
    Entry = sub_batch(42, 1, 0, 40, <<"x">>),
    ?assertEqual({0, [42]}, write_messages(?VERSION_1, self(), undefined, 7, 1,
                                           Entry, Max, <<"s1">>)),
    ?assertEqual(no_message, flush_one()),
    ok.

write_messages_sub_batch_message_count_exceeds_uncompressed_size_rejected(_Config) ->
    Max = 1000,
    %% 100 sub-entries, each at least 4 bytes, cannot fit in 10 uncompressed bytes
    Entry = sub_batch(42, 1, 100, 10, <<"x">>),
    ?assertEqual({0, [42]}, write_messages(?VERSION_1, self(), undefined, 7, 1,
                                           Entry, Max, <<"s1">>)),
    ?assertEqual(no_message, flush_one()),
    ok.

write_messages_sub_batch_unknown_compression_type_rejected(_Config) ->
    Max = 1000,
    [begin
         Entry = sub_batch(42, CompressionType, 1, 40, <<"0123456789">>),
         ?assertEqual({0, [42]}, write_messages(?VERSION_1, self(), undefined, 7, 1,
                                                Entry, Max, <<"s1">>)),
         ?assertEqual(no_message, flush_one())
     end || CompressionType <- [5, 6, 7]],
    ok.

write_messages_sub_batch_empty_batch_with_compression_rejected(_Config) ->
    Max = 1000,
    Entry = sub_batch(42, 1, 1, 40, <<>>),
    ?assertEqual({0, [42]}, write_messages(?VERSION_1, self(), undefined, 7, 1,
                                           Entry, Max, <<"s1">>)),
    ?assertEqual(no_message, flush_one()),
    ok.

write_messages_sub_batch_high_compression_ratio_accepted(_Config) ->
    Max = 67108864,
    Entry = sub_batch(1, 4, 65535, Max, <<"tiny-compressed-payload">>),
    ?assertEqual({1, []}, write_messages(?VERSION_1, self(), undefined, 7, 1,
                                         Entry, Max, <<"s1">>)),
    ?assertMatch({'$gen_cast', _}, flush_one()),
    ok.

%% once an entry fails validation, the rest of the frame is rejected too,
%% instead of being validated and written independently. This avoids leaving
%% a gap in the publishing ID sequence for publishers that rely on order.
write_messages_fail_fast_after_first_invalid_sub_batch(_Config) ->
    Max = 1000,
    Valid1 = simple_entry(1, <<"hello">>),
    Invalid = sub_batch(2, 1, 1, Max + 1, <<"x">>),
    Valid2 = simple_entry(3, <<"world">>),
    Messages = <<Valid1/binary, Invalid/binary, Valid2/binary>>,
    ?assertEqual({1, [2, 3]}, write_messages(?VERSION_1, self(), undefined, 9, 1,
                                             Messages, Max, <<"s1">>)),
    %% only the entry preceding the failure was written
    ?assertMatch({'$gen_cast', {write, _, undefined, {9, 1, 1}, <<"hello">>}},
                 flush_one()),
    ?assertEqual(no_message, flush_one()),
    ok.

write_messages_returns_written_count(_Config) ->
    Max = 1000,
    Entry1 = simple_entry(1, <<"hello">>),
    Entry2 = simple_entry(2, <<"world">>),
    Messages = <<Entry1/binary, Entry2/binary>>,
    ?assertEqual({2, []}, write_messages(?VERSION_1, self(), undefined, 7, 1,
                                         Messages, Max, <<"s1">>)),
    ?assertMatch({'$gen_cast', {write, _, undefined, {7, 1, 1}, <<"hello">>}},
                 flush_one()),
    ?assertMatch({'$gen_cast', {write, _, undefined, {7, 1, 2}, <<"world">>}},
                 flush_one()),
    ?assertEqual(no_message, flush_one()),
    ok.

simple_entry(PublishingId, Message) ->
    MessageSize = byte_size(Message),
    <<PublishingId:64, 0:1, MessageSize:31, Message:MessageSize/binary>>.

sub_batch(PublishingId, CompressionType, MessageCount, UncompressedSize, Batch) ->
    BatchSize = byte_size(Batch),
    <<PublishingId:64, 1:1, CompressionType:3, 0:4, MessageCount:16,
      UncompressedSize:32, BatchSize:32, Batch:BatchSize/binary>>.

flush_one() ->
    receive
        Msg -> Msg
    after 100 ->
        no_message
    end.

binding(Destination, Order) ->
    #binding{destination = #resource{name = Destination},
             args = [{<<"x-stream-partition-order">>, signedint, Order}]}.

binding(Destination) ->
    #binding{destination = #resource{name = Destination}, args = []}.
