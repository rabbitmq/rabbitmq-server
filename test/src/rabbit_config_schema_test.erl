-module(rabbit_config_schema_test).

-export([test_snippet/3]).
-compile(export_all).

test_snippet(Snippet, Expected, Plugins) ->
    {Conf, Advanced} = write_snippet(Snippet),
    rabbit_file:recursive_delete("generated"),
    prepare_schemas(Plugins),
    SchemaDir = rabbit_config:schema_dir(),
    {ok, GeneratedFile} = generate_config(Conf, Advanced, SchemaDir),
    {ok, [Generated]} = file:consult(GeneratedFile),
    Expected = Generated.

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




    

