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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_prelaunch).

-export([start/0, stop/0]).
-export([config_file_check/0]).

-import(rabbit_misc, [pget/2, pget/3]).

-include("rabbit.hrl").

-define(SET_DIST_PORT, 0).
-define(ERROR_CODE, 1).
-define(DO_NOT_SET_DIST_PORT, 2).
-define(EX_USAGE, 64).

%%----------------------------------------------------------------------------

-spec start() -> no_return().

start() ->
    case init:get_plain_arguments() of
        [NodeStr] ->
            Node = rabbit_nodes:make(NodeStr),
            {NodeName, NodeHost} = rabbit_nodes:parts(Node),
            ok = duplicate_node_check(NodeName, NodeHost),
            ok = dist_port_set_check(),
            ok = dist_port_range_check(),
            ok = dist_port_use_check(NodeHost),
            ok = config_file_check();
        [] ->
            %% Ignore running node while installing windows service
            ok = dist_port_set_check(),
            ok
    end,
    rabbit_misc:quit(?SET_DIST_PORT),
    ok.

-spec stop() -> 'ok'.

stop() ->
    ok.

%%----------------------------------------------------------------------------

config_file_check() ->
    case rabbit_config:validate_config_files() of
        ok -> ok;
        {error, {ErrFmt, ErrArgs}} ->
            ErrMsg = io_lib:format(ErrFmt, ErrArgs),
            {{Year, Month, Day}, {Hour, Minute, Second, Milli}} = lager_util:localtime_ms(),
            io:format(standard_error, "~b-~2..0b-~2..0b ~2..0b:~2..0b:~2..0b.~b [error] ~s",
                      [Year, Month, Day, Hour, Minute, Second, Milli, ErrMsg]),
            rabbit_misc:quit(?EX_USAGE)
    end.

%% Check whether a node with the same name is already running
duplicate_node_check(NodeName, NodeHost) ->
    case rabbit_nodes:names(NodeHost) of
        {ok, NamePorts}  ->
            case proplists:is_defined(NodeName, NamePorts) of
                true -> io:format(
                          "ERROR: node with name ~p already running on ~p~n",
                          [NodeName, NodeHost]),
                        rabbit_misc:quit(?ERROR_CODE);
                false -> ok
            end;
        {error, EpmdReason} ->
            io:format("ERROR: epmd error for host ~s: ~s~n",
                      [NodeHost, rabbit_misc:format_inet_error(EpmdReason)]),
            rabbit_misc:quit(?ERROR_CODE)
    end.

dist_port_set_check() ->
    case get_config(os:getenv("RABBITMQ_CONFIG_ARG_FILE")) of
        {ok, [Config]} ->
            Kernel = pget(kernel, Config, []),
            case {pget(inet_dist_listen_min, Kernel, none),
                  pget(inet_dist_listen_max, Kernel, none)} of
                {none, none} -> ok;
                _            -> rabbit_misc:quit(?DO_NOT_SET_DIST_PORT)
            end;
        {ok, _} ->
            ok;
        {error, _} ->
            ok
    end.

get_config("") -> {error, nofile};
get_config(File)  ->
    case consult_file(File) of
        {ok, Contents} -> {ok, Contents};
        {error, _} = E -> E
    end.

consult_file(false) -> {error, nofile};
consult_file(File)  ->
    FileName = case filename:extension(File) of
        ""        -> File ++ ".config";
        ".config" -> File;
        _         -> ""
    end,
    file:consult(FileName).

dist_port_range_check() ->
    case os:getenv("RABBITMQ_DIST_PORT") of
        false   -> ok;
        PortStr -> case catch list_to_integer(PortStr) of
                       Port when is_integer(Port) andalso Port > 65535 ->
                           rabbit_misc:quit(?DO_NOT_SET_DIST_PORT);
                       _ ->
                           ok
                   end
    end.

dist_port_use_check(NodeHost) ->
    case os:getenv("RABBITMQ_DIST_PORT") of
        false   -> ok;
        PortStr -> Port = list_to_integer(PortStr),
		   dist_port_use_check_ipv4(NodeHost, Port)
    end.

dist_port_use_check_ipv4(NodeHost, Port) ->
    case gen_tcp:listen(Port, [inet, {reuseaddr, true}]) of
	{ok, Sock} -> gen_tcp:close(Sock);
	{error, einval} -> dist_port_use_check_ipv6(NodeHost, Port);
	{error, _} -> dist_port_use_check_fail(Port, NodeHost)
    end.

dist_port_use_check_ipv6(NodeHost, Port) ->
    case gen_tcp:listen(Port, [inet6, {reuseaddr, true}]) of
	{ok, Sock} -> gen_tcp:close(Sock);
	{error, _} -> dist_port_use_check_fail(Port, NodeHost)
    end.

-spec dist_port_use_check_fail(non_neg_integer(), string()) ->
                                         no_return().

dist_port_use_check_fail(Port, Host) ->
    {ok, Names} = rabbit_nodes:names(Host),
    case [N || {N, P} <- Names, P =:= Port] of
        []     -> io:format("ERROR: distribution port ~b in use on ~s "
                            "(by non-Erlang process?)~n", [Port, Host]);
        [Name] -> io:format("ERROR: distribution port ~b in use by ~s@~s~n",
                            [Port, Name, Host])
    end,
    rabbit_misc:quit(?ERROR_CODE).
