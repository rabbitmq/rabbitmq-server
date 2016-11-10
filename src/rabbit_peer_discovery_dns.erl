%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_peer_discovery_dns).
-behaviour(rabbit_peer_discovery_backend).

-include("rabbit.hrl").

-export([list_nodes/0, register/0, unregister/0]).
%% for tests
-export([discover_nodes/2, discover_hostnames/2]).

%%
%% API
%%

-spec list_nodes() -> {ok, Nodes :: list()} | {error, Reason :: string()}.

list_nodes() ->
    case application:get_env(rabbit, autocluster) of
      undefined         -> io:format("rabbit.autocluster~n", []), {[], disc};
      {ok, Autocluster} ->
        case proplists:get_value(peer_discovery_dns, Autocluster) of
            undefined -> io:format("rabbit.autocluster.peer_discovery_dns~n", []), {[], disc};
            Proplist  ->
                Hostname = rabbit_data_coercion:to_list(proplists:get_value(hostname, Proplist)),
                NodeType = proplists:get_value(node_type, Autocluster, disc),

                {discover_nodes(Hostname, net_kernel:longnames()), NodeType}
        end
    end.

-spec register() -> ok.

register() ->
    ok.

-spec unregister() -> ok.

unregister() ->
    ok.


%%
%% Implementation
%%

discover_nodes(SeedHostname, LongNamesUsed) ->
    [rabbit_peer_discovery:append_node_prefix(H) ||
        H <- discover_hostnames(SeedHostname, LongNamesUsed)].

discover_hostnames(SeedHostname, LongNamesUsed) ->
    %% TODO: IPv6 support
    Hosts = [extract_host(inet_res:gethostbyaddr(A), LongNamesUsed) ||
                A <- inet_res:lookup(SeedHostname, in, a)],
    lists:filter(fun(E) -> E =/= error end, Hosts).

%% long node names are used
extract_host({ok, {hostent, FQDN, _, _, _, _}}, true) ->
  FQDN;
%% short node names are used
extract_host({ok, {hostent, FQDN, _, _, _, _}}, false) ->
  lists:nth(1, string:tokens(FQDN, "."));
extract_host(Error, _) ->
  rabbit_log:error("DNS peer discovery failed: ~p", [Error]),
  error.
