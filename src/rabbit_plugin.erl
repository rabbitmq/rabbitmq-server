%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_plugin).
-include("rabbit.hrl").

-export([start/0, stop/0]).

-define(FORCE_OPT, "-f").

-record(plugin, {name,          %% atom()
                 version,       %% string()
                 description,   %% string()
                 dependencies,  %% [{atom(), string()}]
                 location}).    %% string()

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start/0 :: () -> no_return()).
-spec(stop/0 :: () -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start() ->
    {ok, [[PluginsDir|_]|_]} = init:get_argument(plugins_dir),
    {ok, [[PluginsDistDir|_]|_]} = init:get_argument(plugins_dist_dir),
    {[Command0 | Args], Opts} =
        case rabbit_misc:get_options([{flag, ?FORCE_OPT}],
                                     init:get_plain_arguments()) of
            {[], _Opts}    -> usage();
            CmdArgsAndOpts -> CmdArgsAndOpts
        end,
    Command = list_to_atom(Command0),

    case catch action(Command, Args, Opts, PluginsDir, PluginsDistDir) of
        ok ->
            rabbit_misc:quit(0);
        {'EXIT', {function_clause, [{?MODULE, action, _} | _]}} ->
            print_error("invalid command '~s'",
                        [string:join([atom_to_list(Command) | Args], " ")]),
            usage();
        {error, Reason} ->
            print_error("~p", [Reason]),
            rabbit_misc:quit(2);
        Other ->
            print_error("~p", [Other]),
            rabbit_misc:quit(2)
    end.

stop() ->
    ok.

print_error(Format, Args) ->
    rabbit_misc:format_stderr("Error: " ++ Format ++ "~n", Args).

usage() ->
    io:format("Insert USAGE here.~n"),
    rabbit_misc:quit(1).

%%----------------------------------------------------------------------------

action(list, [], _Opts, PluginsDir, PluginsDistDir) ->
    io:format("All plugins: ~p~n", [find_available_plugins(PluginsDistDir)]).

%%----------------------------------------------------------------------------

find_available_plugins(PluginsDistDir) ->
    EZs = filelib:wildcard("*.ez", PluginsDistDir),
    [get_plugin_info(filename:join([PluginsDistDir, EZ])) || EZ <- EZs].

get_plugin_info(EZ) ->
    case read_app_file(EZ) of
        {application, Name, Props} ->
            Version = proplists:get_value(vsn, Props),
            Description = proplists:get_value(description, Props, ""),
            Dependencies = proplists:get_value(applications, Props, []),
            #plugin{name = Name, version = Version, description = Description,
                    dependencies = Dependencies, location = EZ};
        {error, _} = Error ->
            Error
    end.

%% Read the .app file from an ez.
read_app_file(EZ) ->
    case zip:list_dir(EZ) of
        {ok, [_|ZippedFiles]} ->
            case find_app_files(ZippedFiles) of
                [AppPath|_] ->
                    {ok, [{AppPath, AppFile}]} =
                        zip:extract(EZ, [{file_list, [AppPath]}, memory]),
                    parse_binary(AppFile);
                [] ->
                    {error, no_app_file}
            end;
        {error, Reason} ->
            {error, {invalid_ez, Reason}}
    end.

%% Return the path of the .app files in ebin/.
find_app_files(ZippedFiles) ->
    {ok, RE} = re:compile("^.*/ebin/.*.app$"),
    [Path || {zip_file, Path, _, _, _, _} <- ZippedFiles,
             re:run(Path, RE, [{capture, none}]) =:= match].

%% Parse a binary into a term.
parse_binary(Bin) ->
    try
        {ok, Ts, _} = erl_scan:string(binary:bin_to_list(Bin)),
        {ok, Term} = erl_parse:parse_term(Ts),
        Term
    catch
        Err -> {error, {invalid_app, Err}}
    end.
