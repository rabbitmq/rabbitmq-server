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

-module(rabbit_nodes).

-export([names/1, diagnostics/1, make/1, parts/1, cookie_hash/0,
         is_running/2, is_process_running/2,
         cluster_name/0, set_cluster_name/1]).

-include_lib("kernel/include/inet.hrl").

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
-spec(is_running/2 :: (node(), atom()) -> boolean()).
-spec(is_process_running/2 :: (node(), atom()) -> boolean()).
-spec(cluster_name/0 :: () -> binary()).
-spec(set_cluster_name/1 :: (binary()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

names(Hostname) ->
    Self = self(),
    Ref = make_ref(),
    {Pid, MRef} = spawn_monitor(
                    fun () -> Self ! {Ref, net_adm:names(Hostname)} end),
    timer:exit_after(?EPMD_TIMEOUT, Pid, timeout),
    receive
        {Ref, Names}                         -> erlang:demonitor(MRef, [flush]),
                                                Names;
        {'DOWN', MRef, process, Pid, Reason} -> {error, Reason}
    end.

diagnostics(Nodes) ->
    io:format("~nDiagnosing connectivity..."),
    NodeDiags = [{" done.~n~nDIAGNOSTICS~n===========~n~n"
                  "attempted to contact: ~p~n", [Nodes]}] ++
        [diagnostics_node(Node) || Node <- Nodes] ++
        current_node_details(),
    rabbit_misc:format_many(lists:flatten(NodeDiags)).

current_node_details() ->
    [{"~ncurrent node details:~n- node name: ~w", [node()]},
     case init:get_argument(home) of
         {ok, [[Home]]} -> {"- home dir: ~s", [Home]};
         Other          -> {"- no home dir: ~p", [Other]}
     end,
     {"- cookie hash: ~s", [cookie_hash()]}].

diagnostics_node(Node) ->
    {Name, Host} = parts(Node),
    case names(Host) of
        {error, EpmdReason} ->
            {"- unable to connect to epmd on ~s: ~w (~s)",
             [Host, EpmdReason, rabbit_misc:format_inet_error(EpmdReason)]};
        {ok, NamePorts} ->
            case [{N, P} || {N, P} <- NamePorts, N =:= Name] of
                [] ->
                    {SelfName, SelfHost} = parts(node()),
                    NamePorts1 = case SelfHost of
                                     Host -> [{N, P} || {N, P} <- NamePorts,
                                                        N =/= SelfName];
                                     _    -> NamePorts
                                 end,
                    case NamePorts1 of
                        [] ->
                            {"- ~s:~n"
                             "  * node seems not to be running at all~n"
                             "  * no other nodes on ~s",
                             [Node, Host]};
                        _ ->
                            {"- ~s:~n"
                             "  * node seems not to be running at all~n"
                             "  * other nodes on ~s: ~p",
                             [Node, Host, fmt_nodes(NamePorts1)]}
                    end;
                [{Name, Port}] ->
                    case diagnose_connect(Host, Port) of
                        ok ->
                            {"- ~s:~n"
                             "  * found ~s~n"
                             "  * TCP connection succeeded~n"
                             "  * suggestion: is the cookie set correctly?~n",
                             [Node, fmt_node({Name, Port})]};
                        {error, Reason} ->
                            {"- ~s:~n"
                             "  * found ~s~n"
                             "  * can't establish TCP connection, reason: ~p~n"
                             "  * suggestion: blocked by firewall?~n",
                             [Node, fmt_node({Name, Port}), Reason]}
                    end
            end
    end.

fmt_nodes(Hs) ->
    [fmt_node(H) || H <- Hs].

fmt_node({Name, Port}) ->
    rabbit_misc:format("~s: port ~b", [Name, Port]).

diagnose_connect(Host, Port) ->
    lists:foldl(fun (_Fam, ok) -> ok;
                    (Fam, _)   -> case gen_tcp:connect(
                                         Host, Port, [Fam], 5000) of
                                      {ok, Socket}   -> gen_tcp:close(Socket),
                                                        ok;
                                      {error, _} = E -> E
                                  end
                end, undefined, [inet6, inet]).

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

is_running(Node, Application) ->
    case rpc:call(Node, rabbit_misc, which_applications, []) of
        {badrpc, _} -> false;
        Apps        -> proplists:is_defined(Application, Apps)
    end.

is_process_running(Node, Process) ->
    case rpc:call(Node, erlang, whereis, [Process]) of
        {badrpc, _}      -> false;
        undefined        -> false;
        P when is_pid(P) -> true
    end.

cluster_name() ->
    rabbit_runtime_parameters:value_global(
      cluster_name, cluster_name_default()).

cluster_name_default() ->
    {ID, _} = rabbit_nodes:parts(node()),
    {ok, Host} = inet:gethostname(),
    {ok, #hostent{h_name = FQDN}} = inet:gethostbyname(Host),
    list_to_binary(atom_to_list(rabbit_nodes:make({ID, FQDN}))).

set_cluster_name(Name) ->
    rabbit_runtime_parameters:set_global(cluster_name, Name).
