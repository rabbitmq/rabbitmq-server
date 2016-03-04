-module(rabbit_config_schema_test).

-export([test_snippet/3]).
-compile(export_all).


run_snippets(FileName) ->
    {ok, [Snippets]} = file:consult(FileName),
    lists:map(
        fun({N,S,C,P})   -> test_snippet({integer_to_list(N),S,[]},C,P);
           ({N,S,A,C,P}) -> test_snippet({integer_to_list(N),S,A},C,P)
        end,
        Snippets).

test_snippet(Snippet, Expected, Plugins) ->
    {Conf, Advanced} = write_snippet(Snippet),
    rabbit_file:recursive_delete("generated"),
    prepare_schemas(Plugins),
    SchemaDir = rabbit_config:schema_dir(),
    {ok, GeneratedFile} = generate_config(Conf, Advanced, SchemaDir),
    {ok, [Generated]} = file:consult(GeneratedFile),
    Gen = deepsort(Generated),
    Exp = deepsort(Expected),
    case Exp of
        Gen -> ok;
        _         -> 
            error({config_mismatch, Snippet, Exp, Gen})
    end.

write_snippet({Name, Config, Advanced}) ->
    rabbit_file:recursive_delete(filename:join(["examples", Name])),
    file:make_dir("examples"),
    file:make_dir(filename:join(["examples", Name])),
    ConfFile = filename:join(["examples", Name, "config.conf"]),
    AdvancedFile = filename:join(["examples", Name, "advanced.config"]),

    file:write_file(ConfFile, Config),
    rabbit_file:write_term_file(AdvancedFile, [Advanced]),
    {ConfFile, AdvancedFile}.
    

generate_config(Conf, Advanced, SchemaDir) ->
    ScriptDir = case init:get_argument(conf_script_dir) of
        {ok, D} -> D;
        _       -> "scripts"
    end,
    rabbit_config:generate_config_file([Conf], ".", ScriptDir, 
                                       SchemaDir, Advanced).

prepare_schemas(Plugins) ->
    {ok, EnabledFile} = application:get_env(rabbit, enabled_plugins_file),
    rabbit_file:write_term_file(EnabledFile, [Plugins]).



deepsort(List) ->
    case is_proplist(List) of
        true ->
            lists:keysort(1, lists:map(fun({K,V}) -> {K, deepsort(V)};
                                          (V) -> V end,
                                       List));
        false -> 
            case is_list(List) of
                true  -> lists:sort(List);
                false -> List
            end
    end.

is_proplist([{_K,_V}|_] = List) -> lists:all(fun({_K,_V}) -> true; (_) -> false end, List);
is_proplist(_) -> false.
