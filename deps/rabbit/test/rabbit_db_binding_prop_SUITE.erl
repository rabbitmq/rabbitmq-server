%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_binding_prop_SUITE).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         tie_binding_to_dest_with_keep_while_cond/1,
         tie_binding_to_dest_with_keep_while_cond1/1]).

-define(VHOST, <<"/">>).
-define(FEATURE_FLAG, tie_binding_to_dest_with_keep_while_cond).
-define(NUM_TESTS, 100).

all() ->
    [{group, property_based}].

groups() ->
    [{property_based, [], [tie_binding_to_dest_with_keep_while_cond]}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodename_suffix, Group},
                         {rmq_nodes_count, 1}]),
    %% The feature flag must start disabled so the property can run the
    %% migration itself.
    Config2 = rabbit_ct_helpers:merge_app_env(
                Config1, {rabbit, [{forced_feature_flags_on_init, []}]}),
    rabbit_ct_helpers:run_steps(
      Config2, rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(
      Config, rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config.

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

tie_binding_to_dest_with_keep_while_cond(Config) ->
    true = rabbit_ct_broker_helpers:rpc(
             Config, 0, ?MODULE,
             tie_binding_to_dest_with_keep_while_cond1, [Config]).

tie_binding_to_dest_with_keep_while_cond1(_Config) ->
    proper:quickcheck(
      prop_tie_binding_to_dest_with_keep_while_cond(),
      [{numtests, ?NUM_TESTS}]).

%% Each exchange is described by whether it is auto-delete and whether it has a
%% binding where it is the source.
exchange_spec() ->
    {boolean(), boolean()}.

topology() ->
    ?LET(N, integer(0, 12), vector(N, exchange_spec())).

%% For any topology, the migration must succeed, keep every exchange, and add a
%% `keep_while' condition to exactly those auto-delete exchanges that have a
%% source binding.
prop_tie_binding_to_dest_with_keep_while_cond() ->
    ?FORALL(
       Specs, topology(),
       begin
           ok = clear(),
           Destination = declare_exchange(<<"x.destination">>, fanout, false),
           Names = declare_topology(Specs, Destination),
           Ret = run_migration(),
           AllExist = lists:all(
                        fun(N) -> rabbit_khepri:exists(xpath(N)) end,
                        [Destination | Names]),
           CondsOk = lists:all(
                       fun({{AutoDelete, HasBinding}, N}) ->
                               has_keep_while_cond(N) =:=
                                   (AutoDelete andalso HasBinding)
                       end,
                       lists:zip(Specs, Names)),
           ok = clear(),
           Ret =:= ok andalso AllExist andalso CondsOk
       end).

declare_topology(Specs, Destination) ->
    [declare_spec(I, Spec, Destination) || {I, Spec} <- lists:enumerate(Specs)].

declare_spec(I, {AutoDelete, HasBinding}, Destination) ->
    XName = declare_exchange(
              <<"x-", (integer_to_binary(I))/binary>>, headers, AutoDelete),
    case HasBinding of
        true  -> ok = bind(XName, Destination);
        false -> ok
    end,
    XName.

declare_exchange(Name, Type, AutoDelete) ->
    XName = rabbit_misc:r(?VHOST, exchange, Name),
    X = #exchange{name = XName, type = Type,
                  durable = true, auto_delete = AutoDelete,
                  decorators = {[], []}},
    {new, #exchange{}} = rabbit_db_exchange:create_or_get(X),
    XName.

bind(SrcName, DstName) ->
    Binding = #binding{source = SrcName, key = <<>>,
                       destination = DstName, args = #{}},
    rabbit_db_binding:create(Binding, fun(_, _) -> ok end).

%% Invokes the migration callback directly. Unlike enabling the feature flag,
%% this can be run repeatedly on a single node, which the property relies on.
run_migration() ->
    rabbit_db_queue:tie_binding_to_dest_with_keep_while_cond_enable(
      #{feature_name => ?FEATURE_FLAG}).

clear() ->
    ok = rabbit_db_exchange:clear(),
    ok = rabbit_db_binding:clear(),
    ok.

xpath(#resource{name = Name}) ->
    rabbit_db_exchange:khepri_exchange_path(?VHOST, Name).

has_keep_while_cond(XName) ->
    {ok, Conds} = khepri_machine:get_keep_while_conds_state(
                    rabbit_khepri:get_store_id(), #{}),
    maps:is_key(xpath(XName), Conds).
