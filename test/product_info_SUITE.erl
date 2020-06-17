%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(product_info_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([suite/0,
         all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         override_product_name_in_conf/1,
         override_product_version_in_conf/1,
         set_motd_in_conf/1
        ]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [
     {group, parallel}
    ].

groups() ->
    [
     {parallel, [],
      [override_product_name_in_conf,
       override_product_version_in_conf,
       set_motd_in_conf]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    case os:type() of
        {unix, _} ->
            rabbit_ct_helpers:log_environment(),
            rabbit_ct_helpers:run_setup_steps(Config);
        _ ->
            {skip, "This testsuite is only relevant on Unix"}
    end.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = 1,
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [
                 {rmq_nodename_suffix, Testcase},
                 {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}}
                ]),
    Config2 = case Testcase of
                  override_product_name_in_conf ->
                      rabbit_ct_helpers:merge_app_env(
                        Config1,
                        {rabbit, [{product_name, "MyProduct"}]});
                  override_product_version_in_conf ->
                      rabbit_ct_helpers:merge_app_env(
                        Config1,
                        {rabbit, [{product_version, "MyVersion"}]});
                  set_motd_in_conf ->
                      PrivDir = ?config(priv_dir, Config),
                      MotdFile = filename:join(PrivDir, "motd.txt"),
                      ok = file:write_file(MotdFile, <<"My MOTD\n">>),
                      C2 = rabbit_ct_helpers:set_config(
                             Config1,
                             {motd_file, MotdFile}),
                      rabbit_ct_helpers:merge_app_env(
                        C2,
                        {rabbit, [{motd_file, MotdFile}]})
              end,
    rabbit_ct_helpers:run_steps(Config2,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

override_product_name_in_conf(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ProductName = "MyProduct",
    ?assertEqual(ProductName,
                 rabbit_ct_broker_helpers:rpc(
                   Config, A, rabbit, product_name, [])),
    grep_in_log_file(Config, A, ProductName),
    grep_in_stdout(Config, A, ProductName).

override_product_version_in_conf(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ProductVersion = "MyVersion",
    ?assertEqual(ProductVersion,
                 rabbit_ct_broker_helpers:rpc(
                   Config, A, rabbit, product_version, [])),
    grep_in_log_file(Config, A, ProductVersion),
    grep_in_stdout(Config, A, ProductVersion).

set_motd_in_conf(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    MotdFile = ?config(motd_file, Config),
    ?assertEqual(MotdFile,
                 rabbit_ct_broker_helpers:rpc(
                   Config, A, rabbit, motd_file, [])),
    {ok, Motd0} = file:read_file(MotdFile),
    Motd = string:trim(Motd0, trailing, [$\r,$\n]),
    ?assertEqual(Motd,
                 rabbit_ct_broker_helpers:rpc(
                   Config, A, rabbit, motd, [])),
    grep_in_log_file(Config, A, Motd),
    grep_in_stdout(Config, A, Motd).

grep_in_log_file(Config, Node, String) ->
    [Log | _] = rabbit_ct_broker_helpers:rpc(
                  Config, Node, rabbit, log_locations, []),
    ct:pal(?LOW_IMPORTANCE, "Grepping \"~s\" in ~s", [String, Log]),
    %% We try to grep several times, in case the log file was not
    %% fsync'd yet (and thus we don't see the content yet).
    do_grep_in_log_file(String, Log, 30).

do_grep_in_log_file(String, Log, Retries) ->
    {ok, Content} = file:read_file(Log),
    case re:run(Content, ["\\b", String, "\\b"], [{capture, none}]) of
        match ->
            ok;
        nomatch when Retries > 1 ->
            timer:sleep(1000),
            do_grep_in_log_file(String, Log, Retries - 1);
        nomatch ->
            throw({failed_to_grep, String, Log, Content})
    end.

grep_in_stdout(Config, Node, String) ->
    [Log | _] = rabbit_ct_broker_helpers:rpc(
                  Config, Node, rabbit, log_locations, []),
    LogDir = filename:dirname(Log),
    Stdout = filename:join(LogDir, "startup_log"),
    ct:pal(?LOW_IMPORTANCE, "Grepping \"~s\" in ~s", [String, Stdout]),
    {ok, Content} = file:read_file(Stdout),
    ?assertMatch(
       match,
       re:run(Content, ["\\b", String, "\\b"], [{capture, none}])).
