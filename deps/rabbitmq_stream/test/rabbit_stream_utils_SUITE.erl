-module(rabbit_stream_utils_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [{group, tests}].

suite() ->
    [{timetrap, {seconds, 30}}].

groups() ->
    [{tests, [], [sort_partitions, filter_spec, filter_defined]}].

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

binding(Destination, Order) ->
    #binding{destination = #resource{name = Destination},
             args = [{<<"x-stream-partition-order">>, signedint, Order}]}.

binding(Destination) ->
    #binding{destination = #resource{name = Destination}, args = []}.
