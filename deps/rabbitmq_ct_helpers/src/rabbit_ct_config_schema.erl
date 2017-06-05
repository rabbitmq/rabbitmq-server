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
%% Copyright (c) 2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_ct_config_schema).
-include_lib("common_test/include/ct.hrl").

-export([init_schemas/2]).
-export([run_snippets/1]).

init_schemas(App, Config) ->
    DepsDir = ?config(erlang_mk_depsdir, Config),
    RabbitSchema = filename:join([DepsDir, "rabbit", "priv", "schema", "rabbitmq.schema"]),
    Schemas = case App of
        rabbit -> [RabbitSchema];
        _      -> [RabbitSchema,
                   filename:join([DepsDir, App, "priv", "schema", atom_to_list(App) ++ ".schema"])]
    end,
    ct:pal("Schemas ~p~n", [Schemas]),
    SchemaDir = filename:join(?config(data_dir, Config), "schema"),
    file:make_dir(SchemaDir),
    ct:pal("Schema DIR ~p~n", [SchemaDir]),
    [ copy_to(Schema, SchemaDir) || Schema <- Schemas ],

    ResultsDir = filename:join(?config(priv_dir, Config), "results"),
    Snippets = filename:join(?config(data_dir, Config),
                              atom_to_list(App) ++ ".snippets"),
    ok = file:make_dir(ResultsDir),

    rabbit_ct_helpers:set_config(Config, [
        {schema_dir, SchemaDir},
        {results_dir, ResultsDir},
        {conf_snippets, Snippets}
        ]).

copy_to(File, Dir) ->
    BaseName = filename:basename(File),
    {ok, _} = file:copy(File, Dir ++ "/" ++ BaseName).

run_snippets(Config) ->
    {ok, [Snippets]} = file:consult(?config(conf_snippets, Config)),
    lists:map(
        fun({N, S, C, P})    -> ok = test_snippet(Config, {snippet_id(N), S, []}, C, P);
           ({N, S, A, C, P}) -> ok = test_snippet(Config, {snippet_id(N), S, A},  C, P)
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

test_snippet(Config, Snippet, Expected, _Plugins) ->
    {ConfFile, AdvancedFile} = write_snippet(Config, Snippet),
    {ok, GeneratedFile} = generate_config(Config, ConfFile, AdvancedFile),
    {ok, [Generated]} = file:consult(GeneratedFile),
    Gen = deepsort(Generated),
    Exp = deepsort(Expected),
    case Exp of
        Gen -> ok;
        _         ->
            ct:pal("Expected: ~p, generated: ~p", [Exp, Gen]),
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
