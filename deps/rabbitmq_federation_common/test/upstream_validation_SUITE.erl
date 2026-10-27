%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(upstream_validation_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(ITERATIONS, 100).

-define(USER, <<"upstream-validation-user">>).
-define(PASSWORD, <<"upstream-validation-password">>).
-define(ALLOWED_VHOST, <<"upstream-validation-allowed">>).
-define(DENIED_VHOST, <<"upstream-validation-denied">>).
-define(NONEXISTENT_VHOST, <<"upstream-validation-nonexistent">>).

all() ->
    [
      {group, tests}
    ].

groups() ->
    [
      {tests, [], [
          direct_uri_allowed,
          direct_uri_denied,
          direct_uri_nonexistent_vhost,
          direct_uri_no_user,
          direct_uri_without_username_denied,
          direct_uri_own_username_without_password_denied,
          direct_uri_other_username_denied,
          direct_uri_other_username_and_password_allowed,
          direct_uri_administrator_allowed,
          network_uri_allowed,
          loopback_user_denied,
          uri_list_denied,
          uri_list_allowed,
          uri_list_empty,
          uri_list_nested,
          map_uri_denied,
          uri_list_mixed_network_and_denied_direct,
          uri_too_long,
          uri_connection_timeout_too_large,
          invalid_uri_error_has_no_credentials,
          prop_uri_list_validation
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [{rmq_nodename_suffix, ?MODULE}]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps() ++
      [fun setup_vhosts_and_user/1]).

setup_vhosts_and_user(Config) ->
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?ALLOWED_VHOST),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?DENIED_VHOST),
    ok = rabbit_ct_broker_helpers:add_user(Config, ?USER, ?PASSWORD),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, ?USER, ?ALLOWED_VHOST),
    Config.

end_per_suite(Config) ->
    rabbit_ct_broker_helpers:delete_user(Config, ?USER),
    rabbit_ct_broker_helpers:delete_vhost(Config, ?ALLOWED_VHOST),
    rabbit_ct_broker_helpers:delete_vhost(Config, ?DENIED_VHOST),
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases
%% -------------------------------------------------------------------

direct_uri_allowed(Config) ->
    ?assertEqual([ok], validate_with_user(Config, direct_uri(?ALLOWED_VHOST))).

direct_uri_denied(Config) ->
    ?assertMatch([{error, _, _}], validate_with_user(Config, direct_uri(?DENIED_VHOST))).

direct_uri_nonexistent_vhost(Config) ->
    ?assertMatch([{error, _, _}], validate_with_user(Config, direct_uri(?NONEXISTENT_VHOST))).

direct_uri_no_user(Config) ->
    ?assertEqual([ok], validate_with_none(Config, direct_uri(?DENIED_VHOST))).

direct_uri_without_username_denied(Config) ->
    ?assertMatch([{error, _, _}],
                 validate_with_user(Config, <<"amqp:///", ?ALLOWED_VHOST/binary>>)).

direct_uri_own_username_without_password_denied(Config) ->
    [?assertMatch([{error, _, _}], validate_with_user(Config, Uri))
     || Uri <- [<<"amqp://", ?USER/binary, "@/", ?ALLOWED_VHOST/binary>>,
                <<"amqp://", ?USER/binary, ":@/", ?ALLOWED_VHOST/binary>>]].

direct_uri_other_username_denied(Config) ->
    ?assertMatch([{error, _, _}],
                 validate_with_user(Config, <<"amqp://other@/", ?ALLOWED_VHOST/binary>>)),
    ?assertMatch([{error, _, _}],
                 validate_with_user(Config, <<"amqp://other:@/", ?ALLOWED_VHOST/binary>>)).

direct_uri_other_username_and_password_allowed(Config) ->
    ?assertEqual([ok],
                 validate_with_user(Config, <<"amqp://other:pass@/", ?ALLOWED_VHOST/binary>>)).

direct_uri_administrator_allowed(Config) ->
    Uris = [<<"amqp:///", ?ALLOWED_VHOST/binary>>,
            <<"amqp://other@/", ?ALLOWED_VHOST/binary>>,
            <<"amqp://localhost:5672/", ?DENIED_VHOST/binary>>],
    ok = rabbit_ct_broker_helpers:set_user_tags(Config, 0, ?USER, [administrator]),
    try
        ?assertEqual([ok], validate_with_user(Config, Uris))
    after
        ok = rabbit_ct_broker_helpers:set_user_tags(Config, 0, ?USER, [])
    end.

network_uri_allowed(Config) ->
    ?assertEqual([ok], validate_with_user(Config, network_uri(?DENIED_VHOST))).

loopback_user_denied(Config) ->
    [?assertMatch([{error, _, _}], validate_with_user(Config, Uri))
     || Uri <- [<<"amqp://localhost:5672/", ?DENIED_VHOST/binary>>,
                <<"amqp://guest:guest@localhost:5672/", ?DENIED_VHOST/binary>>,
                <<"amqp://guest:guest@/", ?ALLOWED_VHOST/binary>>]].

uri_list_denied(Config) ->
    Uris = [direct_uri(?ALLOWED_VHOST), direct_uri(?DENIED_VHOST)],
    ?assertMatch([{error, _, _}], validate_with_user(Config, Uris)).

uri_list_allowed(Config) ->
    Uris = [direct_uri(?ALLOWED_VHOST), direct_uri(?ALLOWED_VHOST)],
    ?assertEqual([ok], validate_with_user(Config, Uris)).

uri_list_empty(Config) ->
    ?assertMatch([{error, _, _}], validate_with_user(Config, [])),
    ?assertMatch([{error, _, _}], validate_with_none(Config, [])).

uri_list_nested(Config) ->
    [begin
         ?assertMatch([{error, _, _}], validate_with_user(Config, Uris)),
         ?assertMatch([{error, _, _}], validate_with_none(Config, Uris))
     end || Uris <- [[[]],
                     [[network_uri(?ALLOWED_VHOST)]],
                     [network_uri(?ALLOWED_VHOST), [network_uri(?ALLOWED_VHOST)]]]].

map_uri_denied(Config) ->
    Term = #{<<"uri">> => direct_uri(?DENIED_VHOST)},
    ?assertMatch([{error, _, _}], validate_term_with_user(Config, Term)).

uri_list_mixed_network_and_denied_direct(Config) ->
    Uris = [network_uri(?DENIED_VHOST), direct_uri(?DENIED_VHOST)],
    ?assertMatch([{error, _, _}], validate_with_user(Config, Uris)).

uri_too_long(Config) ->
    ?assertEqual([ok], validate_with_user(Config, network_uri_of_size(65536))),
    ?assertMatch([{error, _, [<<"uri">>, 65536]}],
                 validate_with_user(Config, network_uri_of_size(65537))),
    ?assertMatch([{error, _, [<<"uri">>, 65536]}],
                 validate_with_user(Config, [network_uri(?ALLOWED_VHOST),
                                             network_uri_of_size(65537)])).

uri_connection_timeout_too_large(Config) ->
    Uri = fun(Timeout) ->
                  Base = network_uri(?ALLOWED_VHOST),
                  <<Base/binary, "?connection_timeout=",
                    (integer_to_binary(Timeout))/binary>>
          end,
    ?assertEqual([ok], validate_with_user(Config, Uri(180000))),
    ?assertMatch([{error, _, [<<"uri">>, 180000]}],
                 validate_with_user(Config, Uri(180001))),
    ?assertMatch([{error, _, [<<"uri">>, 180000]}],
                 validate_with_none(Config, [Uri(1000), Uri(180001)])).

invalid_uri_error_has_no_credentials(Config) ->
    [begin
         [{error, Fmt, Args}] = validate_with_none(Config, Uri),
         Msg = lists:flatten(io_lib:format(Fmt, Args)),
         ?assertEqual(nomatch, string:find(Msg, "s3cret"))
     end || Uri <- [<<"amqp://alice:s3cret@:::">>,
                    <<"amqps://alice:s3cret@host/?depth=x&password=s3cret">>,
                    <<"amqp://alice:s3cret%zz@host">>,
                    [[<<"amqp://alice:s3cret@host">>]]]],
    ok.

%% -------------------------------------------------------------------
%% Property-based test
%% -------------------------------------------------------------------

%% Runs on the broker node so each example is a local call, not an RPC.
prop_uri_list_validation(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, ?MODULE, run_prop_uri_list_validation, []).

run_prop_uri_list_validation() ->
    {ok, User} = rabbit_access_control:check_user_login(?USER, [{password, ?PASSWORD}]),
    rabbit_ct_proper_helpers:run_proper(
      fun() ->
              ?FORALL(
                 Choices, non_empty(list(oneof([allowed, denied]))),
                 begin
                     Uris = [direct_uri(vhost_for(Choice)) || Choice <- Choices],
                     Result = validate(Uris, User),
                     ExpectAllowed = lists:all(fun(C) -> C =:= allowed end, Choices),
                     outcome(Result) =:= (case ExpectAllowed of
                                              true  -> allowed;
                                              false -> denied
                                          end)
                 end)
      end, [], ?ITERATIONS).

outcome([ok]) -> allowed;
outcome([{error, _, _}]) -> denied.

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

vhost_for(allowed)     -> ?ALLOWED_VHOST;
vhost_for(denied)      -> ?DENIED_VHOST;
vhost_for(nonexistent) -> ?NONEXISTENT_VHOST.

direct_uri(VHost) ->
    <<"amqp://", ?USER/binary, ":", ?PASSWORD/binary, "@/", VHost/binary>>.

network_uri(VHost) ->
    <<"amqp://", ?USER/binary, ":", ?PASSWORD/binary, "@localhost:5672/", VHost/binary>>.

network_uri_of_size(Size) ->
    Prefix = <<"amqp://", ?USER/binary, ":", ?PASSWORD/binary, "@localhost?">>,
    <<Prefix/binary, (binary:copy(<<"&">>, Size - byte_size(Prefix)))/binary>>.

validate_with_user(Config, Uris) ->
    validate_term_with_user(Config, [{<<"uri">>, Uris}]).

validate_term_with_user(Config, Term) ->
    rabbit_ct_broker_helpers:rpc(Config, ?MODULE, validate_term_with_user_1, [Term]).

validate_term_with_user_1(Term) ->
    {ok, User} = rabbit_access_control:check_user_login(?USER, [{password, ?PASSWORD}]),
    validate_term(Term, User).

validate_with_none(Config, Uris) ->
    rabbit_ct_broker_helpers:rpc(Config, ?MODULE, validate_with_none_1, [Uris]).

validate_with_none_1(Uris) ->
    validate(Uris, none).

validate(Uris, User) ->
    validate_term([{<<"uri">>, Uris}], User).

validate_term(Term, User) ->
    rabbit_federation_parameters:validate(
      <<"/">>, <<"federation-upstream">>, <<"a-name">>, Term, User).
