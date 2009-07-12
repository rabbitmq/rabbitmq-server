%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_plugin).

-export([start_plugins/0]).

%% TODO Think of something better than this name, probablt somewhere in /etc
-define(PLUGIN_CONF_DIR, "plugins").


%% Loads shared libraries and plugins that exist in the plugin dir
start_plugins() ->
    io:format("~nstarting plugins...~n"),
    [begin
        [_Dir,PluginString|_] = string:tokens(Config,"/."),
        Plugin = list_to_atom(PluginString),
        case parse_plugin_config(PluginString) of
            ok ->
                ensure_dependencies(Plugin),
                case application:start(Plugin) of
                    {error, Reason} ->
                        rabbit_log:error("Error starting ~p plugin: "
                                         "~p~n", [Plugin, Reason]);
                    _ ->
                        io:format("...started ~p plugin ~n", [Plugin])
                end;
            _ -> ok
        end
    end || Config <- filelib:wildcard("plugins/*.ez")],
    io:format("...done~n").
    
%% Reads the application descriptor and makes sure all of the applications
%% it depends on are loaded
ensure_dependencies(Plugin) when is_atom(Plugin)->
    case application:load(Plugin) of
        ok -> ok;
        {error, {already_loaded, Plugin}} -> ok;
        {error, Reason} ->
            rabbit_log:error("Error loading descriptor for ~p plugin: "
                             "~p~n", [Plugin, Reason]),
            exit(plugin_not_loadable)
    end,
    {ok, Required} = application:get_key(Plugin, applications),
    {Running, _, _} = lists:unzip3(application:which_applications()),
    [case lists:member(App, Running) of
        true  -> ok;
        false -> application:start(App)
    end || App <- Required].

parse_plugin_config(Plugin) when is_list(Plugin)->
    Atom = list_to_atom(Plugin),
    Conf = ?PLUGIN_CONF_DIR ++ "/" ++ Plugin ++ ".cfg",
    case file:consult(Conf) of
        {ok, Terms} ->
            lists:foreach(fun({K,V}) ->
                             application:set_env(Atom, K, V)
                          end, Terms),
            ok;
        {error, enoent} ->
            rabbit_log:warning("Could not locate a config file for the ~p "
                               "plugin, this might be normal though~n", [Atom]),
            ok;
        {error, _} ->
            rabbit_log:error("Error accessing config file for ~p
                              plugin, ", [Atom]),
            error
    end.
