-module(rabbit_stream_utils_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-import(rabbit_stream_utils,
        [validate_super_stream_max_partitions/2]).

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
       super_stream_partition_helpers]}].

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

binding(Destination, Order) ->
    #binding{destination = #resource{name = Destination},
             args = [{<<"x-stream-partition-order">>, signedint, Order}]}.

binding(Destination) ->
    #binding{destination = #resource{name = Destination}, args = []}.
