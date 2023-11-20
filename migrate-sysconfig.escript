#!/usr/bin/env escript
%%! -sname migrate_sysconfig -hidden

main([NodeStr]) ->
    Node = list_to_atom(NodeStr),
    {ok, [[RootDir]]} = erpc:call(Node, init, get_argument, [root]),
    Apps = [App
            || {App, _, _} <- erpc:call(
                                Node, application, which_applications, [])],
    AppConfigs = lists:sort(
                   lists:foldl(
                     fun(App, Acc) ->
                             Ret = erpc:call(
                                     Node, application, get_all_env, [App]),
                             case Ret of
                                 []  -> Acc;
                                 Env -> [{App, Env} | Acc]
                             end
                     end, [], Apps)),
    SysConfig = io_lib:format("~p.~n", [AppConfigs]),
    RelsDir = filename:join(RootDir, "releases"),
    RelDirs = [Dir
               || Dir <- filelib:wildcard(filename:join(RelsDir, "*")),
                  filelib:is_dir(Dir)],
    lists:foreach(
      fun(RelDir) ->
              SysConfigFile = filename:join([RelDir, "sys.config"]),
              file:write_file(SysConfigFile, SysConfig)
      end, RelDirs).
