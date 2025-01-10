-module(globber_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("rabbitmq_mqtt/include/rabbit_globber.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [{group, tests}].

groups() ->
    [{tests, [shuffle], [new]}].

new() ->
    Globber = rabbit_globber:new(),
    ?assertEqual(#globber{}, Globber),
    Globber2 = rabbit_globber:new(<<"/">>, <<"*">>, <<"#">>),
    ?assertEqual(#globber{separator = <<"/">>,
                          wildcard_one = <<"*">>,
                          wildcard_some = <<"#">>},
                 Globber2).

add() ->
    Globber = rabbit_globber:new(),
    Globber1 = rabbit_globber:add(Globber, <<"test.*">>, <<"matches">>),
    ?assertMatch(#globber{trie = _}, Globber1),
    Globber2 = rabbit_globber:add(Globber1, <<"test.#">>, <<"it n">>),
    ?assertMatch(#globber{trie = _}, Globber2).

remove() ->
    Globber = rabbit_globber:new(),
    Globber1 = rabbit_globber:add(Globber, <<"test.*">>, <<"matches">>),
    Globber2 = rabbit_globber:remove(Globber1, <<"test.*">>, <<"matches">>),
    ?assertEqual(Globber, Globber2).

match() ->
    Globber = rabbit_globber:new(),
    Globber1 = rabbit_globber:add(Globber, <<"test.*">>, <<"it matches">>),
    Result = rabbit_globber:match(Globber1, <<"test.bar">>),
    ?assertEqual([<<"it matches">>], Result),
    Result2 = rabbit_globber:match(Globber1, <<"test.foo">>),
    ?assertEqual([<<"it matches">>], Result2),
    Result3 = rabbit_globber:match(Globber1, <<"not.foo">>),
    ?assertEqual([], Result3).

test() ->
    Globber = rabbit_globber:new(),
    Globber1 = rabbit_globber:add(Globber, <<"test.*">>),
    ?assertEqual(true, rabbit_globber:test(Globber1, <<"test.bar">>)),
    ?assertEqual(false, rabbit_globber:test(Globber1, <<"foo.bar">>)).

match_iter() ->
    Globber = rabbit_globber:new(),
    Globber1 = rabbit_globber:add(Globber, <<"test.*">>, <<"matches">>),
    Result = rabbit_globber:match_iter(Globber1, <<"test.bar">>),
    ?assertEqual([<<"matches">>], Result).

clear() ->
    Globber = rabbit_globber:new(),
    Globber1 = rabbit_globber:add(Globber, <<"test.*">>, <<"matches">>),
    Globber2 = rabbit_globber:clear(Globber1),
    ?assertEqual(Globber, Globber2).

multiple_patterns() ->
    Globber = rabbit_globber:new(<<".">>, <<"*">>, <<"#">>),
    Globber1 = rabbit_globber:add(Globber, <<"foo.#">>, <<"catchall">>),
    Globber2 = rabbit_globber:add(Globber1, <<"foo.*.bar">>, <<"single_wildcard">>),
    Globber3 = rabbit_globber:add(Globber2, <<"foo.*.bar.#">>, <<"single_and_catchall">>),

    ?assertEqual([<<"catchall">>], rabbit_globber:match(Globber3, <<"foo.bar">>)),
    ?assertEqual([<<"catchall">>], rabbit_globber:match(Globber3, <<"foo.bar.baz">>)),

    ?assertEqual([<<"catchall">>, <<"single_wildcard">>],
                 rabbit_globber:match(Globber3, <<"foo.test.bar">>)),
    ?assertEqual([<<"catchall">>], rabbit_globber:match(Globber3, <<"foo.test.baz.bar">>)),

    ?assertEqual([<<"catchall">>, <<"single_and_catchall">>],
                 rabbit_globber:match(Globber3, <<"foo.test.bar.baz">>)),
    ?assertEqual([<<"catchall">>, <<"single_and_catchall">>],
                 rabbit_globber:match(Globber3, <<"foo.test.bar.baz.qux">>)).
