%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(default_queue_type_prop_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-import(rabbit_ct_broker_helpers, [rpc/5]).

-compile(nowarn_export_all).
-compile(export_all).

all() ->
    [
     prop_inject_dqt_output_invariants,
     prop_inject_dqt_preserves_valid_types
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1}
    ]),
    rabbit_ct_helpers:run_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase},
        {rmq_nodes_count, 1}
    ]),
    Config2 = rabbit_ct_helpers:run_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()),
    rabbit_ct_helpers:testcase_started(Config2, Testcase).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Property-based Tests
%% -------------------------------------------------------------------

%% Property: inject_dqt always produces valid output regardless of input DQT value
prop_inject_dqt_output_invariants(Config) ->
    Property = fun() -> prop_output_invariants(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], 100).

prop_output_invariants(Config) ->
    ?FORALL({Name, DQT, ExtraMetaKeys},
            {vhost_name_gen(), dqt_gen(), list(atom())},
        begin
            ExtraMeta = maps:from_list([{K, K} || K <- ExtraMetaKeys]),
            Input = case DQT of
                none -> #{name => Name, metadata => ExtraMeta};
                Val -> #{name => Name, default_queue_type => Val, metadata => ExtraMeta}
            end,
            Result = rpc(Config, 0, rabbit_queue_type, inject_dqt, [Input]),

            maps:is_key(default_queue_type, Result) andalso
            begin
                Meta = maps:get(metadata, Result, #{}),
                maps:is_key(default_queue_type, Meta) andalso
                lists:all(fun(K) -> maps:is_key(K, Meta) end, ExtraMetaKeys)
            end
        end).

%% Property: valid queue types are preserved, invalid ones fall back to default
prop_inject_dqt_preserves_valid_types(Config) ->
    Property = fun() -> prop_preserves_valid_types(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], 100).

prop_preserves_valid_types(Config) ->
    Default = rpc(Config, 0, rabbit_queue_type, default_alias, []),
    ?FORALL(DQT, dqt_gen(),
        begin
            Input = case DQT of
                none -> #{name => <<"/">>};
                Val -> #{name => <<"/">>, default_queue_type => Val}
            end,
            #{default_queue_type := ResultDQT} =
                rpc(Config, 0, rabbit_queue_type, inject_dqt, [Input]),

            case DQT of
                none -> ResultDQT =:= Default;
                undefined -> ResultDQT =:= Default;
                <<"undefined">> -> ResultDQT =:= Default;
                "undefined" -> ResultDQT =:= Default;
                <<"classic">> -> ResultDQT =:= <<"classic">>;
                <<"quorum">> -> ResultDQT =:= <<"quorum">>;
                <<"stream">> -> ResultDQT =:= <<"stream">>
            end
        end).

%% -------------------------------------------------------------------
%% Generators
%% -------------------------------------------------------------------

vhost_name_gen() ->
    ?LET(Name, non_empty(binary()), <<"/", Name/binary>>).

dqt_gen() ->
    oneof([
        none,
        undefined,
        <<"undefined">>,
        "undefined",
        <<"classic">>,
        <<"quorum">>,
        <<"stream">>
    ]).
