%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%


-module(term_to_binary_compat_prop_SUITE).

-compile(export_all).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-define(ITERATIONS_TO_RUN_UNTIL_CONFIDENT, 10000).

all() ->
    [
        ensure_term_to_binary_defaults_to_version_1,
        term_to_binary_latin_atom,
        queue_name_to_binary
    ].

erts_gt_8() ->
    Vsn = erlang:system_info(version),
    [Maj|_] = string:tokens(Vsn, "."),
    list_to_integer(Maj) > 8.

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

%% R16B03 defaults term_to_binary version to 0, this test would always fail
ensure_term_to_binary_defaults_to_version_1(Config) ->
    CurrentERTS = erlang:system_info(version),
    MinimumTestedERTS = "6.0",
    case rabbit_misc:version_compare(CurrentERTS, MinimumTestedERTS, gte) of
        true ->
            Property = fun () ->
                        prop_ensure_term_to_binary_defaults_to_version_1(Config)
                       end,
            rabbit_ct_proper_helpers:run_proper(
                Property, [],
                ?ITERATIONS_TO_RUN_UNTIL_CONFIDENT);
        false ->
            ct:pal(
              ?LOW_IMPORTANCE,
              "This test require ERTS ~p or above, running on ~p~n"
              "Skipping test...",
              [MinimumTestedERTS, CurrentERTS])
    end.

prop_ensure_term_to_binary_defaults_to_version_1(_Config) ->
    ?FORALL(Term, any(),
        begin
            Current = term_to_binary(Term),
            Compat = term_to_binary_compat:term_to_binary_1(Term),
            Current =:= Compat
        end).

term_to_binary_latin_atom(Config) ->
    Property = fun () -> prop_term_to_binary_latin_atom(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [],
                                        ?ITERATIONS_TO_RUN_UNTIL_CONFIDENT).

prop_term_to_binary_latin_atom(_Config) ->
    ?FORALL(LatinString, list(integer(0, 255)),
        begin
            Length = length(LatinString),
            Atom = list_to_atom(LatinString),
            Binary = list_to_binary(LatinString),
            <<131,100, Length:16, Binary/binary>> =:=
                term_to_binary_compat:term_to_binary_1(Atom)
        end).

queue_name_to_binary(Config) ->
    Property = fun () -> prop_queue_name_to_binary(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [],
                                        ?ITERATIONS_TO_RUN_UNTIL_CONFIDENT).


prop_queue_name_to_binary(_Config) ->
    ?FORALL({VHost, QName}, {binary(), binary()},
            begin
                VHostBSize = byte_size(VHost),
                NameBSize = byte_size(QName),
                Expected =
                    <<131,                               %% Binary format "version"
                      104, 4,                            %% 4-element tuple
                      100, 0, 8, "resource",             %% `resource` atom
                      109, VHostBSize:32, VHost/binary,  %% Vhost binary
                      100, 0, 5, "queue",                %% `queue` atom
                      109, NameBSize:32, QName/binary>>, %% Name binary
                Resource = rabbit_misc:r(VHost, queue, QName),
                Current = term_to_binary_compat:term_to_binary_1(Resource),
                Current =:= Expected
            end).
