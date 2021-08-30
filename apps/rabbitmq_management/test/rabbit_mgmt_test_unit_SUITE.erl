%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_test_unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, parallel_tests}
    ].

groups() ->
    [
     {parallel_tests, [parallel], [
                                   tokenise_test,
                                   pack_binding_test,
                                   path_prefix_test
                                  ]}
    ].

%% -------------------------------------------------------------------
%% Setup/teardown.
%% -------------------------------------------------------------------

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

tokenise_test(_Config) ->
    [] = rabbit_mgmt_format:tokenise(""),
    ["foo"] = rabbit_mgmt_format:tokenise("foo"),
    ["foo", "bar"] = rabbit_mgmt_format:tokenise("foo~bar"),
    ["foo", "", "bar"] = rabbit_mgmt_format:tokenise("foo~~bar"),
    ok.

pack_binding_test(_Config) ->
    assert_binding(<<"~">>,
                   <<"">>, []),
    assert_binding(<<"foo">>,
                   <<"foo">>, []),
    assert_binding(<<"foo%7Ebar%2Fbash">>,
                   <<"foo~bar/bash">>, []),
    assert_binding(<<"foo%7Ebar%7Ebash">>,
                   <<"foo~bar~bash">>, []),
    ok.

path_prefix_test(_Config) ->
    Got0 = rabbit_mgmt_util:get_path_prefix(),
    ?assertEqual("", Got0),

    Pfx0 = "/custom-prefix",
    application:set_env(rabbitmq_management, path_prefix, Pfx0),
    Got1 = rabbit_mgmt_util:get_path_prefix(),
    ?assertEqual(Pfx0, Got1),

    Pfx1 = "custom-prefix",
    application:set_env(rabbitmq_management, path_prefix, Pfx1),
    Got2 = rabbit_mgmt_util:get_path_prefix(),
    ?assertEqual(Pfx0, Got2),

    Pfx2 = <<"custom-prefix">>,
    application:set_env(rabbitmq_management, path_prefix, Pfx2),
    Got3 = rabbit_mgmt_util:get_path_prefix(),
    ?assertEqual(Pfx0, Got3).

%%--------------------------------------------------------------------

assert_binding(Packed, Routing, Args) ->
    case rabbit_mgmt_format:pack_binding_props(Routing, Args) of
        Packed ->
            ok;
        Act ->
            throw({pack, Routing, Args, expected, Packed, got, Act})
    end.
