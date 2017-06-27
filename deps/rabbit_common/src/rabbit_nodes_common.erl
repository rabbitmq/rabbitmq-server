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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_nodes_common).

-export([make/1, parts/1, ensure_epmd/0]).

-spec make({string(), string()} | string()) -> node().
-spec parts(node() | string()) -> {string(), string()}.
-spec ensure_epmd() -> 'ok'.

make({Prefix, Suffix}) -> list_to_atom(lists:append([Prefix, "@", Suffix]));
make(NodeStr)          -> make(parts(NodeStr)).

parts(Node) when is_atom(Node) ->
    parts(atom_to_list(Node));
parts(NodeStr) ->
    case lists:splitwith(fun (E) -> E =/= $@ end, NodeStr) of
        {Prefix, []}     -> {_, Suffix} = parts(node()),
                            {Prefix, Suffix};
        {Prefix, Suffix} -> {Prefix, tl(Suffix)}
    end.

ensure_epmd() ->
    {ok, Prog} = init:get_argument(progname),
    ID = rabbit_misc:random(1000000000),
    Port = open_port(
             {spawn_executable, os:find_executable(Prog)},
             [{args, ["-sname", rabbit_misc:format("epmd-starter-~b", [ID]),
                      "-noshell", "-eval", "halt()."]},
              exit_status, stderr_to_stdout, use_stdio]),
    port_shutdown_loop(Port).

port_shutdown_loop(Port) ->
    receive
        {Port, {exit_status, _Rc}} -> ok;
        {Port, _}                  -> port_shutdown_loop(Port)
    end.
