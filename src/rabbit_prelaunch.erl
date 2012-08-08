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
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_prelaunch).

-export([start/0, stop/0]).

-include("rabbit.hrl").

-define(BaseApps, [rabbit]).
-define(ERROR_CODE, 1).

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
    ok = duplicate_node_check(NodeStr),
    rabbit_misc:quit(0),
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
                false -> ok
            end;
        {error, EpmdReason} ->
            io:format("ERROR: epmd error for host ~p: ~p (~s)~n",
                      [NodeHost, EpmdReason,
                       rabbit_misc:format_inet_error(EpmdReason)]),
            rabbit_misc:quit(?ERROR_CODE)
    end.
