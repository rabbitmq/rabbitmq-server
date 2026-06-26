%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_web_dispatch_websocket_origin_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

-define(M, rabbit_web_dispatch_websocket_origin).
-define(SERVER, {<<"http">>, <<"localhost">>, 15674}).

all() ->
    [{group, unit}, {group, properties}].

groups() ->
    [{unit, [parallel],
      [no_restriction_allows_any_origin,
       allowlist_accepts_listed_origin,
       allowlist_rejects_unlisted_origin,
       missing_origin_allowed_without_strict,
       missing_origin_rejected_with_strict,
       same_origin_accepts_matching_origin,
       same_origin_rejects_different_port,
       same_origin_rejects_different_host,
       same_origin_rejects_different_scheme,
       same_origin_accepts_default_ports,
       same_origin_is_case_insensitive,
       same_origin_rejects_opaque_origin,
       same_origin_rejects_malformed_origin,
       permissive_warning_only_for_allow_all]},
     {properties, [parallel],
      [prop_same_origin_matches_self,
       prop_same_origin_rejects_other_port,
       prop_same_origin_is_case_insensitive]}].

init_per_suite(Config) -> Config.
end_per_suite(Config) -> Config.
init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.
init_per_testcase(_, Config) -> Config.
end_per_testcase(_, Config) -> Config.

%%
%% Unit tests
%%

%% The default (no allowlist) accepts every Origin, including a missing one;
%% strict mode is a no-op because there is no allowlist to enforce.
no_restriction_allows_any_origin(_Config) ->
    ?assertEqual(ok, ?M:validate(<<"https://evil.example.com">>, ?SERVER, [], false)),
    ?assertEqual(ok, ?M:validate(undefined, ?SERVER, [], false)),
    ?assertEqual(ok, ?M:validate(<<"https://evil.example.com">>, ?SERVER, [], true)),
    ?assertEqual(ok, ?M:validate(undefined, ?SERVER, [], true)).

allowlist_accepts_listed_origin(_Config) ->
    List = ["https://a.example.com", "https://b.example.com"],
    ?assertEqual(ok, ?M:validate(<<"https://b.example.com">>, ?SERVER, List, false)).

allowlist_rejects_unlisted_origin(_Config) ->
    List = ["https://a.example.com"],
    ?assertEqual({error, origin_not_allowed},
                 ?M:validate(<<"https://evil.example.com">>, ?SERVER, List, false)).

missing_origin_allowed_without_strict(_Config) ->
    ?assertEqual(ok, ?M:validate(undefined, ?SERVER, ["https://a.example.com"], false)),
    ?assertEqual(ok, ?M:validate(undefined, ?SERVER, same_origin, false)).

missing_origin_rejected_with_strict(_Config) ->
    ?assertEqual({error, origin_not_allowed},
                 ?M:validate(undefined, ?SERVER, ["https://a.example.com"], true)),
    ?assertEqual({error, origin_not_allowed},
                 ?M:validate(undefined, ?SERVER, same_origin, true)).

same_origin_accepts_matching_origin(_Config) ->
    ?assertEqual(ok, ?M:validate(<<"http://localhost:15674">>, ?SERVER, same_origin, false)).

same_origin_rejects_different_port(_Config) ->
    ?assertEqual({error, origin_not_allowed},
                 ?M:validate(<<"http://localhost:15670">>, ?SERVER, same_origin, false)).

same_origin_rejects_different_host(_Config) ->
    ?assertEqual({error, origin_not_allowed},
                 ?M:validate(<<"http://other:15674">>, ?SERVER, same_origin, false)).

same_origin_rejects_different_scheme(_Config) ->
    ?assertEqual({error, origin_not_allowed},
                 ?M:validate(<<"https://localhost:15674">>, ?SERVER, same_origin, false)).

%% An Origin without an explicit port matches a server on the scheme's
%% default port (80 for http, 443 for https).
same_origin_accepts_default_ports(_Config) ->
    ?assertEqual(ok, ?M:validate(<<"http://example.com">>,
                                 {<<"http">>, <<"example.com">>, 80}, same_origin, false)),
    ?assertEqual(ok, ?M:validate(<<"https://example.com">>,
                                 {<<"https">>, <<"example.com">>, 443}, same_origin, false)).

same_origin_is_case_insensitive(_Config) ->
    ?assertEqual(ok, ?M:validate(<<"HTTP://LOCALHOST:15674">>, ?SERVER, same_origin, false)).

%% Browsers send "Origin: null" from sandboxed/opaque contexts; it is never
%% same-origin.
same_origin_rejects_opaque_origin(_Config) ->
    ?assertEqual({error, origin_not_allowed},
                 ?M:validate(<<"null">>, ?SERVER, same_origin, false)).

%% A value that is not a parseable absolute URI has no scheme/host.
same_origin_rejects_malformed_origin(_Config) ->
    ?assertEqual({error, origin_not_allowed},
                 ?M:validate(<<"not a url">>, ?SERVER, same_origin, false)).

permissive_warning_only_for_allow_all(_Config) ->
    ?assertNotEqual(none, ?M:permissive_warning(<<"Web STOMP:">>, [])),
    ?assertEqual(none, ?M:permissive_warning(<<"Web STOMP:">>, same_origin)),
    ?assertEqual(none, ?M:permissive_warning(<<"Web STOMP:">>, ["https://a.example.com"])).

%%
%% Properties — fuzz the same-origin URI parsing and comparison
%%

prop_same_origin_matches_self(_Config) ->
    ?assert(quickcheck(
        ?FORALL(T, origin_triple(),
                ?M:validate(build_origin(T), T, same_origin, false) =:= ok))).

prop_same_origin_rejects_other_port(_Config) ->
    ?assert(quickcheck(
        ?FORALL(T = {Scheme, Host, Port}, origin_triple(),
                ?M:validate(build_origin(T),
                            {Scheme, Host, other_port(Port)},
                            same_origin, false) =:= {error, origin_not_allowed}))).

prop_same_origin_is_case_insensitive(_Config) ->
    ?assert(quickcheck(
        ?FORALL(T = {Scheme, Host, Port}, origin_triple(),
                ?M:validate(build_origin({string:uppercase(Scheme),
                                          string:uppercase(Host), Port}),
                            T, same_origin, false) =:= ok))).

%%
%% Generators and helpers
%%

scheme() -> elements([<<"http">>, <<"https">>]).

host() ->
    ?LET(S, non_empty(list(elements("abcdefghijklmnopqrstuvwxyz0123456789"))),
         list_to_binary(S)).

origin_triple() -> {scheme(), host(), range(1, 65535)}.

build_origin({Scheme, Host, Port}) ->
    <<Scheme/binary, "://", Host/binary, ":", (integer_to_binary(Port))/binary>>.

other_port(65535) -> 1;
other_port(Port)  -> Port + 1.

quickcheck(Prop) ->
    proper:quickcheck(Prop, [{numtests, 500}, {to_file, user}]).
