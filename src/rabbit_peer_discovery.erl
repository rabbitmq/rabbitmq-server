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
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_peer_discovery).

%% API
-export([discover_cluster_nodes/0, backend/0,
         normalize/1, format_discovered_nodes/1]).
-export([append_node_prefix/1, node_prefix/0]).


-spec backend() -> atom().

backend() ->
  case application:get_env(rabbit, peer_discovery_backend) of
    {ok, Backend} when is_atom(Backend) -> Backend;
    undefined                           -> rabbit_peer_discovery_classic_config
  end.


-spec discover_cluster_nodes() -> {ok, Nodes :: list()} |
                                  {ok, {Nodes :: list(), NodeType :: rabbit_types:node_type()}} |
                                  {error, Reason :: string()}.

discover_cluster_nodes() ->
    Backend = backend(),
    normalize(Backend:list_nodes()).


-spec normalize({ok, Nodes :: list()} |
                {ok, {Nodes :: list(), NodeType :: rabbit_types:node_type()}} |
                {error, Reason :: string()}) -> {ok, {Nodes :: list(), NodeType :: rabbit_types:node_type()}} |
                                                {error, Reason :: string()}.

normalize({ok, Nodes}) when is_list(Nodes) ->
  {ok, {Nodes, disc}};
normalize({ok, {Nodes, NodeType}}) when is_list(Nodes) andalso is_atom(NodeType) ->
  {ok, {Nodes, NodeType}};
normalize({error, Reason}) ->
  {error, Reason}.


-spec format_discovered_nodes(Nodes :: list()) -> string().

format_discovered_nodes(Nodes) ->
  string:join(lists:map(fun (Val) -> hd(io_lib:format("~s", [Val])) end, Nodes), ", ").


-define(DEFAULT_PREFIX, "rabbit").
-define(NODENAME_PART_SEPARATOR, "@").


-spec node_prefix() -> string().

node_prefix() ->
    case string:tokens(atom_to_list(node()), ?NODENAME_PART_SEPARATOR) of
        [Prefix, _] -> Prefix;
        [_]         -> ?DEFAULT_PREFIX
    end.



-spec append_node_prefix(Value :: binary() | list()) -> atom().

append_node_prefix(Value) ->
    Val = rabbit_data_coercion:to_list(Value),
    Hostname = case string:tokens(Val, ?NODENAME_PART_SEPARATOR) of
                   [_ExistingPrefix, Val] ->
                       Val;
                   [Val]                  ->
                       Val
               end,
    string:join([node_prefix(), Hostname], ?NODENAME_PART_SEPARATOR).
