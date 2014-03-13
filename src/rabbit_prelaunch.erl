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
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_prelaunch).

-export([start/0, stop/0]).

-import(rabbit_misc, [pget/2, pget/3]).

-include("rabbit.hrl").

-define(DIST_PORT_NOT_CONFIGURED, 0).
-define(ERROR_CODE, 1).
-define(DIST_PORT_CONFIGURED, 2).

%%----------------------------------------------------------------------------
%% Specs
%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start/0 :: () -> no_return()).
-spec(stop/0 :: () -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start() ->
    [NodeStr] = init:get_plain_arguments(),
    {ok, NodeHost} = duplicate_node_check(NodeStr),
    ok = dist_port_set_check(),
    ok = dist_port_use_check(NodeHost),
    rabbit_misc:quit(?DIST_PORT_NOT_CONFIGURED),
    ok.

stop() ->
    ok.

%%----------------------------------------------------------------------------

%% Check whether a node with the same name is already running
duplicate_node_check([]) ->
    %% Ignore running node while installing windows service
    ok;
duplicate_node_check(NodeStr) ->
    Node = rabbit_nodes:make(NodeStr),
    {NodeName, NodeHost} = rabbit_nodes:parts(Node),
    case rabbit_nodes:names(NodeHost) of
        {ok, NamePorts}  ->
            case proplists:is_defined(NodeName, NamePorts) of
                true -> io:format("ERROR: node with name ~p "
                                  "already running on ~p~n",
                                  [NodeName, NodeHost]),
                        io:format(rabbit_nodes:diagnostics([Node]) ++ "~n"),
                        rabbit_misc:quit(?ERROR_CODE);
                false -> {ok, NodeHost}
            end;
        {error, EpmdReason} ->
            io:format("ERROR: epmd error for host ~s: ~s~n",
                      [NodeHost, rabbit_misc:format_inet_error(EpmdReason)]),
            rabbit_misc:quit(?ERROR_CODE)
    end.

dist_port_set_check() ->
    case os:getenv("RABBITMQ_CONFIG_FILE") of
        false ->
            ok;
        File ->
            case file:consult(File ++ ".config") of
                {ok, [Config]} ->
                    Kernel = pget(kernel, Config, []),
                    case {pget(inet_dist_listen_min, Kernel, none),
                          pget(inet_dist_listen_max, Kernel, none)} of
                        {none, none} -> ok;
                        _            -> rabbit_misc:quit(?DIST_PORT_CONFIGURED)
                    end;
                {error, _} ->
                    %% TODO can we present errors more nicely here
                    %% than after -config has failed?
                    ok
            end
    end.

dist_port_use_check(NodeHost) ->
    case os:getenv("RABBITMQ_DIST_PORT") of
        false   -> ok;
        PortStr -> Port = list_to_integer(PortStr),
                   case gen_tcp:listen(Port, [inet]) of
                       {ok, Sock} -> gen_tcp:close(Sock);
                       {error, _} -> dist_port_use_check_fail(Port, NodeHost)
                   end
    end.

dist_port_use_check_fail(Port, Host) ->
    {ok, Names} = rabbit_nodes:names(Host),
    case [N || {N, P} <- Names, P =:= Port] of
        []     -> io:format("ERROR: distribution port ~b in use on ~s "
                            "(by non-Erlang process?)~n", [Port, Host]);
        [Name] -> io:format("ERROR: distribution port ~b in use by ~s@~s~n",
                            [Port, Name, Host])
    end,
    rabbit_misc:quit(?ERROR_CODE).
