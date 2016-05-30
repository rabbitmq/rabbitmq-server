%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(config_schema_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          run_snippets
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase}
      ]),
    Config2 = case Testcase of
        run_snippets ->
            SchemaDir = filename:join(?config(data_dir, Config1), "schema"),
            ResultsDir = filename:join(?config(priv_dir, Config1), "results"),
            Snippets = filename:join(?config(data_dir, Config1),
              "snippets.config"),
            ok = file:make_dir(ResultsDir),
            rabbit_ct_helpers:set_config(Config1, [
                {schema_dir, SchemaDir},
                {results_dir, ResultsDir},
                {conf_snippets, Snippets}
              ])
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

run_snippets(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, run_snippets1, [Config]).

run_snippets1(Config) ->
    {ok, [Snippets]} = file:consult(?config(conf_snippets, Config)),
    lists:map(
        fun({N, S, C, P})    -> ok = test_snippet(Config, {integer_to_list(N), S, []}, C, P);
           ({N, S, A, C, P}) -> ok = test_snippet(Config, {integer_to_list(N), S, A},  C, P)
        end,
        Snippets),
    passed.

test_snippet(Config, Snippet, Expected, _Plugins) ->
    {ConfFile, AdvancedFile} = write_snippet(Config, Snippet),
    {ok, GeneratedFile} = generate_config(Config, ConfFile, AdvancedFile),
    {ok, [Generated]} = file:consult(GeneratedFile),
    Gen = deepsort(Generated),
    Exp = deepsort(Expected),
    case Exp of
        Gen -> ok;
        _         ->
            error({config_mismatch, Snippet, Exp, Gen})
    end.

write_snippet(Config, {Name, Conf, Advanced}) ->
    ResultsDir = ?config(results_dir, Config),
    file:make_dir(filename:join(ResultsDir, Name)),
    ConfFile = filename:join([ResultsDir, Name, "config.conf"]),
    AdvancedFile = filename:join([ResultsDir, Name, "advanced.config"]),

    file:write_file(ConfFile, Conf),
    rabbit_file:write_term_file(AdvancedFile, [Advanced]),
    {ConfFile, AdvancedFile}.

generate_config(Config, ConfFile, AdvancedFile) ->
    SchemaDir = ?config(schema_dir, Config),
    ResultsDir = ?config(results_dir, Config),
    Rabbitmqctl = ?config(rabbitmqctl_cmd, Config),
    ScriptDir = filename:dirname(Rabbitmqctl),
    ct:pal("ConfFile=~p ScriptDir=~p SchemaDir=~p AdvancedFile=~p", [ConfFile, ScriptDir, SchemaDir, AdvancedFile]),
    rabbit_config:generate_config_file([ConfFile], ResultsDir, ScriptDir,
                                       SchemaDir, AdvancedFile).

deepsort(List) ->
    case is_proplist(List) of
        true ->
            lists:keysort(1, lists:map(fun({K, V}) -> {K, deepsort(V)};
                                          (V) -> V end,
                                       List));
        false ->
            case is_list(List) of
                true  -> lists:sort(List);
                false -> List
            end
    end.

is_proplist([{_Key, _Val}|_] = List) -> lists:all(fun({_K, _V}) -> true; (_) -> false end, List);
is_proplist(_) -> false.
