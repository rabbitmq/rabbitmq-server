%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_test_unit_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, parallel_tests},
     {group, sequential_tests}
    ].

groups() ->
    [
     {parallel_tests, [parallel], [
                                   tokenise_test,
                                   pack_binding_test,
                                   default_restrictions,
                                   path_prefix_test,
                                   regex_dos_test
                                  ]},
     {sequential_tests, [], [
                              referrer_policy_header_set_when_configured,
                              referrer_policy_header_absent_when_not_configured,
                              allow_header_absent_on_non_405_when_hide_configured,
                              allow_header_present_on_405_when_hide_configured,
                              allow_header_present_on_200_when_not_configured
                             ]}
    ].

%% -------------------------------------------------------------------
%% Setup/teardown.
%% -------------------------------------------------------------------

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(_, Config) ->
  case application:get_all_env(rabbitmq_management) of
    {error, _} = Error -> Error;
    Env ->
      lists:foreach(fun({Key,_Value})->
          application:unset_env(rabbitmq_management, Key) end, Env),
      case application:get_all_env(rabbitmq_auth_backend_oauth2) of
        {error, _} = Error -> Error;
        Env2 -> lists:foreach(fun({Key,_Value})->
            application:unset_env(rabbitmq_auth_backend_oauth2, Key) end, Env2)
      end
  end,
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

default_restrictions(_) ->
    ?assertEqual(false, rabbit_mgmt_features:is_op_policy_updating_disabled()).

regex_dos_test(_) ->
    %% Verify that a normal regex still works
    NormalRegex = "^test-queue$",
    ?assertEqual(true, rabbit_mgmt_util:maybe_filter_by_keyword(
        name, NormalRegex, [{name, <<"test-queue">>}], "true")),
    ?assertEqual(false, rabbit_mgmt_util:maybe_filter_by_keyword(
        name, NormalRegex, [{name, <<"other-queue">>}], "true")),

    %% A catastrophic backtracking regex hits the match limit on non-matching input
    %% and returns {error, _}. The mitigation maps this to false instead of hanging.
    EvilRegex = "^(a+)+$",
    TargetString = <<"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaac">>,
    ?assertEqual(false, rabbit_mgmt_util:maybe_filter_by_keyword(
        name, EvilRegex, [{name, TargetString}], "true")).

referrer_policy_header_set_when_configured(_Config) ->
    application:set_env(rabbitmq_management, headers,
                        [{referrer_policy, "no-referrer"}]),
    Req = rabbit_mgmt_headers:set_common_permission_headers(fake_req(), ?MODULE),
    RespHeaders = maps:get(resp_headers, Req),
    ?assertEqual(<<"no-referrer">>, maps:get(<<"referrer-policy">>, RespHeaders)).

referrer_policy_header_absent_when_not_configured(_Config) ->
    Req = rabbit_mgmt_headers:set_common_permission_headers(fake_req(), ?MODULE),
    RespHeaders = maps:get(resp_headers, Req),
    ?assertNot(maps:is_key(<<"referrer-policy">>, RespHeaders)).

allow_header_absent_on_non_405_when_hide_configured(_Config) ->
    application:set_env(rabbitmq_management, hide_http_allow_header, true),
    Headers = #{<<"allow">> => <<"GET, HEAD, PUT, DELETE, OPTIONS">>,
                <<"content-type">> => <<"application/json">>},
    Result = rabbit_cowboy_stream_h:maybe_strip_allow_header(200, Headers),
    ?assertNot(maps:is_key(<<"allow">>, Result)),
    ?assert(maps:is_key(<<"content-type">>, Result)).

allow_header_present_on_405_when_hide_configured(_Config) ->
    application:set_env(rabbitmq_management, hide_http_allow_header, true),
    Headers = #{<<"allow">> => <<"GET, HEAD, PUT, DELETE, OPTIONS">>},
    Result = rabbit_cowboy_stream_h:maybe_strip_allow_header(405, Headers),
    ?assert(maps:is_key(<<"allow">>, Result)).

allow_header_present_on_200_when_not_configured(_Config) ->
    application:unset_env(rabbitmq_management, hide_http_allow_header),
    Headers = #{<<"allow">> => <<"GET, HEAD, PUT, DELETE, OPTIONS">>},
    Result = rabbit_cowboy_stream_h:maybe_strip_allow_header(200, Headers),
    ?assert(maps:is_key(<<"allow">>, Result)).

%%--------------------------------------------------------------------

assert_binding(Packed, Routing, Args) ->
    case rabbit_mgmt_format:pack_binding_props(Routing, Args) of
        Packed ->
            ok;
        Act ->
            throw({pack, Routing, Args, expected, Packed, got, Act})
    end.

fake_req() ->
    #{
        resp_headers => #{},
        headers      => #{},
        method       => <<"GET">>,
        scheme       => <<"http">>,
        port         => 15672
    }.
