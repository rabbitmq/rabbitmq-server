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

-module(rabbit_nodes).

-export([names/1, diagnostics/1, make/1, parts/1, cookie_hash/0]).
-export([start_net_kernel/1]).

-define(EPMD_TIMEOUT, 30000).

%%----------------------------------------------------------------------------
%% Specs
%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(names/1 :: (string()) -> rabbit_types:ok_or_error2(
                                 [{string(), integer()}], term())).
-spec(diagnostics/1 :: ([node()]) -> string()).
-spec(make/1 :: ({string(), string()} | string()) -> node()).
-spec(parts/1 :: (node() | string()) -> {string(), string()}).
-spec(cookie_hash/0 :: () -> string()).
-spec(start_net_kernel/1 :: (string()) -> 'ok' | no_return()).

-endif.

%%----------------------------------------------------------------------------

names(Hostname) ->
    Self = self(),
    process_flag(trap_exit, true),
    Pid = spawn_link(fun () -> Self ! {names, net_adm:names(Hostname)} end),
    timer:exit_after(?EPMD_TIMEOUT, Pid, timeout),
    Res = receive
              {names, Names}        -> Names;
              {'EXIT', Pid, Reason} -> {error, Reason}
          end,
    process_flag(trap_exit, false),
    Res.

diagnostics(Nodes) ->
    Hosts = lists:usort([element(2, parts(Node)) || Node <- Nodes]),
    NodeDiags = [{"~nDIAGNOSTICS~n===========~n~n"
                  "nodes in question: ~p~n~n"
                  "hosts, their running nodes and ports:", [Nodes]}] ++
        [diagnostics_host(Host) || Host <- Hosts] ++
        diagnostics0(),
    lists:flatten([io_lib:format(F ++ "~n", A) || NodeDiag <- NodeDiags,
                                                  {F, A}   <- [NodeDiag]]).

diagnostics0() ->
    [{"~ncurrent node details:~n- node name: ~w", [node()]},
     case init:get_argument(home) of
         {ok, [[Home]]} -> {"- home dir: ~s", [Home]};
         Other          -> {"- no home dir: ~p", [Other]}
     end,
     {"- cookie hash: ~s", [cookie_hash()]}].

diagnostics_host(Host) ->
    case names(Host) of
        {error, EpmdReason} ->
            {"- unable to connect to epmd on ~s: ~w",
             [Host, EpmdReason]};
        {ok, NamePorts} ->
            {"- ~s: ~p",
             [Host, [{list_to_atom(Name), Port} ||
                        {Name, Port} <- NamePorts]]}
    end.

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

cookie_hash() ->
    base64:encode_to_string(erlang:md5(atom_to_list(erlang:get_cookie()))).

start_net_kernel(NodeNamePrefix) ->
    {ok, Hostname} = inet:gethostname(),
    MyNodeName = make({NodeNamePrefix ++ os:getpid(), Hostname}),
    case net_kernel:start([MyNodeName, shortnames]) of
        {ok, _} ->
            ok;
        {error, Reason = {shutdown, {child, undefined,
                                     net_sup_dynamic, _, _, _, _, _}}} ->
            Port = case os:getenv("ERL_EPMD_PORT") of
                       false -> 4369;
                       P     -> P
                   end,
            rabbit_misc:format_stderr(
              "Error: epmd could not be contacted: ~p~n", [Reason]),
            rabbit_misc:format_stderr(
              "Check your network setup (in particular "
              "check you can contact port ~w on localhost).~n", [Port]),
            rabbit_misc:quit(1);
        {error, Reason} ->
            rabbit_misc:format_stderr(
              "Error: Networking failed to start: ~p~n", [Reason]),
            rabbit_misc:quit(1)
    end.
