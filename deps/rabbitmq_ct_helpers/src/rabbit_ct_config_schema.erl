%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_ct_config_schema).
-include_lib("common_test/include/ct.hrl").

-export([init_schemas/2]).
-export([run_snippets/1]).

init_schemas(App, Config) ->
    ResultsDir = filename:join(?config(priv_dir, Config), "results"),
    Snippets = filename:join(?config(data_dir, Config),
                              atom_to_list(App) ++ ".snippets"),
    ok = file:make_dir(ResultsDir),
    rabbit_ct_helpers:set_config(Config, [
        {results_dir, ResultsDir},
        {conf_snippets, Snippets}
        ]).

run_snippets(Config) ->
    {ok, [Snippets]} = file:consult(?config(conf_snippets, Config)),
    ct:pal("Loaded config schema snippets: ~tp", [Snippets]),
    %% Snippets may reference relative paths (e.g. cert files).
    %% Set CWD to the plugin source directory so they resolve.
    case ?config(current_srcdir, Config) of
        undefined -> ok;
        SrcDir -> ok = file:set_cwd(SrcDir)
    end,
    %% Parse schemas once and reuse across all snippets to avoid
    %% re-discovering and re-parsing all schema files per snippet.
    Schema = load_schema(),
    lists:foreach(
      fun({N, S, C, P}) ->
              ok = test_snippet(Config, Schema, {snippet_id(N), S, []}, C, P, true);
         ({N, S, A, C, P}) ->
              ok = test_snippet(Config, Schema, {snippet_id(N), S, A},  C, P, true);
         ({N, S, A, C, P, nosort}) ->
              ok = test_snippet(Config, Schema, {snippet_id(N), S, A},  C, P, false)
      end,
      Snippets),
    ok.

snippet_id(N) when is_integer(N) ->
    integer_to_list(N);
snippet_id(F) when is_float(F) ->
    float_to_list(F);
snippet_id(A) when is_atom(A) ->
    atom_to_list(A);
snippet_id(L) when is_list(L) ->
    L.

test_snippet(Config, Schema, Snippet = {SnipID, _, _}, Expected, _Plugins, Sort) ->
    {ConfFile, AdvancedFile} = write_snippet(Config, Snippet),
    %% We ignore the rabbit -> log portion of the config on v3.9+, where the lager
    %% dependency has been dropped
    Generated = case code:which(lager) of
                    non_existing ->
                        without_rabbit_log(generate_config(Schema, ConfFile, AdvancedFile));
                    _ ->
                        generate_config(Schema, ConfFile, AdvancedFile)
                end,
    {Exp, Gen} = case Sort of
                     true ->
                         {deepsort(Expected), deepsort(Generated)};
                     false ->
                         {Expected, Generated}
                 end,
    case Exp of
        Gen -> ok;
        _         ->
            ct:pal("Snippet ~tp. Expected: ~tp~ngenerated: ~tp", [SnipID, Expected, Generated]),
            ct:pal("Snippet ~tp. Expected (sorted): ~tp~ngenerated (sorted): ~tp", [SnipID, Exp, Gen]),
            error({config_mismatch, Snippet, Exp, Gen})
    end.

write_snippet(Config, {Name, Conf, Advanced}) ->
    ResultsDir = ?config(results_dir, Config),
    _ = file:make_dir(filename:join(ResultsDir, Name)),
    ConfFile = filename:join([ResultsDir, Name, "config.conf"]),
    AdvancedFile = filename:join([ResultsDir, Name, "advanced.config"]),

    ok = file:write_file(ConfFile, Conf),
    ok = rabbit_file:write_term_file(AdvancedFile, [Advanced]),
    {ConfFile, AdvancedFile}.

generate_config(Schema, ConfFile, AdvancedFile) ->
    case cuttlefish_conf:files([ConfFile]) of
        {errorlist, Errors} ->
            throw({error, {failed_to_parse_configuration_file, Errors}});
        Conf ->
            Config = case cuttlefish_generator:map(Schema, Conf) of
                         {error, Phase, {errorlist, Errors}} ->
                             throw({error, {failed_to_prepare_configuration, Phase, Errors}});
                         ValidConfig ->
                             proplists:delete(vm_args, ValidConfig)
                     end,
            case rabbit_prelaunch_file:consult_file(AdvancedFile) of
                {ok, [AdvancedConfig]} ->
                    cuttlefish_advanced:overlay(Config, AdvancedConfig);
                {ok, _} ->
                    Config;
                {error, _} ->
                    Config
            end
    end.

load_schema() ->
    Context = rabbit_env:get_context(),
    SchemaFiles = rabbit_prelaunch_conf:find_cuttlefish_schemas(Context),
    cuttlefish_schema:files(SchemaFiles).

without_rabbit_log(ErlangConfig) ->
    case proplists:get_value(rabbit, ErlangConfig) of
        undefined ->
            ErlangConfig;
        RabbitConfig ->
            RabbitConfig1 = lists:keydelete(log, 1, RabbitConfig),
            case RabbitConfig1 of
                [] ->
                    lists:keydelete(rabbit, 1, ErlangConfig);
                _ ->
                    lists:keystore(rabbit, 1, ErlangConfig,
                                   {rabbit, RabbitConfig1})
            end
    end.

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
