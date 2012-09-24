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

-module(rabbit_node_monitor).

-behaviour(gen_server).

-export([running_nodes_filename/0,
         cluster_status_filename/0,
         prepare_cluster_status_files/0,
         write_cluster_status/1,
         read_cluster_status/0,
         update_cluster_status/0,
         reset_cluster_status/0,

         joined_cluster/2,
         notify_joined_cluster/0,
         left_cluster/1,
         notify_left_cluster/1,
         node_up/2,
         notify_node_up/0,

         start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-define(SERVER, ?MODULE).
-define(RABBIT_UP_RPC_TIMEOUT, 2000).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(running_nodes_filename/0 :: () -> string()).
-spec(cluster_status_filename/0 :: () -> string()).
-spec(prepare_cluster_status_files/0 :: () -> 'ok').
-spec(write_cluster_status/1 :: (rabbit_mnesia:cluster_status()) -> 'ok').
-spec(read_cluster_status/0 :: () -> rabbit_mnesia:cluster_status()).
-spec(update_cluster_status/0 :: () -> 'ok').
-spec(reset_cluster_status/0 :: () -> 'ok').

-spec(joined_cluster/2 :: (node(), boolean()) -> 'ok').
-spec(notify_joined_cluster/0 :: () -> 'ok').
-spec(left_cluster/1 :: (node()) -> 'ok').
-spec(notify_left_cluster/1 :: (node()) -> 'ok').
-spec(node_up/2 :: (node(), boolean()) -> 'ok').
-spec(notify_node_up/0 :: () -> 'ok').

-endif.

%%----------------------------------------------------------------------------
%% Cluster file operations
%%----------------------------------------------------------------------------

%% The cluster file information is kept in two files.  The "cluster status file"
%% contains all the clustered nodes and the disc nodes.  The "running nodes
%% file" contains the currently running nodes or the running nodes at shutdown
%% when the node is down.
%%
%% We strive to keep the files up to date and we rely on this assumption in
%% various situations. Obviously when mnesia is offline the information we have
%% will be outdated, but it can't be otherwise.

running_nodes_filename() ->
    filename:join(rabbit_mnesia:dir(), "nodes_running_at_shutdown").

cluster_status_filename() ->
    rabbit_mnesia:dir() ++ "/cluster_nodes.config".

prepare_cluster_status_files() ->
    rabbit_mnesia:ensure_mnesia_dir(),
    CorruptFiles = fun () -> throw({error, corrupt_cluster_status_files}) end,
    RunningNodes1 = case try_read_file(running_nodes_filename()) of
                        {ok, [Nodes]} when is_list(Nodes) -> Nodes;
                        {ok, _      }                     -> CorruptFiles();
                        {error, enoent}                   -> []
                    end,
    {AllNodes1, WantDiscNode} =
        case try_read_file(cluster_status_filename()) of
            {ok, [{AllNodes, DiscNodes0}]} ->
                {AllNodes, lists:member(node(), DiscNodes0)};
            {ok, [AllNodes0]} when is_list(AllNodes0) ->
                {legacy_cluster_nodes(AllNodes0),
                 legacy_should_be_disc_node(AllNodes0)};
            {ok, _} ->
                CorruptFiles();
            {error, enoent} ->
                {legacy_cluster_nodes([]), true}
        end,

    ThisNode = [node()],

    RunningNodes2 = lists:usort(RunningNodes1 ++ ThisNode),
    AllNodes2 = lists:usort(AllNodes1 ++ RunningNodes2),
    DiscNodes = case WantDiscNode of
                    true  -> ThisNode;
                    false -> []
                end,

    ok = write_cluster_status({AllNodes2, DiscNodes, RunningNodes2}).

write_cluster_status({All, Disc, Running}) ->
    ClusterStatusFN = cluster_status_filename(),
    Res = case rabbit_file:write_term_file(ClusterStatusFN, [{All, Disc}]) of
              ok ->
                  RunningNodesFN = running_nodes_filename(),
                  {RunningNodesFN,
                   rabbit_file:write_term_file(RunningNodesFN, [Running])};
              E1 = {error, _} ->
                  {ClusterStatusFN, E1}
          end,
    case Res of
        {_, ok}           -> ok;
        {FN, {error, E2}} -> throw({error, {could_not_write_file, FN, E2}})
    end.

try_read_file(FileName) ->
    case rabbit_file:read_term_file(FileName) of
        {ok, Term}      -> {ok, Term};
        {error, enoent} -> {error, enoent};
        {error, E}      -> throw({error, {cannot_read_file, FileName, E}})
    end.

read_cluster_status() ->
    case {try_read_file(cluster_status_filename()),
          try_read_file(running_nodes_filename())} of
        {{ok, [{All, Disc}]}, {ok, [Running]}} when is_list(Running) ->
            {All, Disc, Running};
        {_, _} ->
            throw({error, corrupt_or_missing_cluster_files})
    end.

update_cluster_status() ->
    {ok, Status} = rabbit_mnesia:cluster_status_from_mnesia(),
    write_cluster_status(Status).

reset_cluster_status() ->
    write_cluster_status({[node()], [node()], [node()]}).

%%----------------------------------------------------------------------------
%% Cluster notifications
%%----------------------------------------------------------------------------

joined_cluster(Node, IsDiscNode) ->
    gen_server:cast(?SERVER, {rabbit_join, Node, IsDiscNode}).

notify_joined_cluster() ->
    cluster_multicall(joined_cluster, [node(), rabbit_mnesia:is_disc_node()]),
    ok.

left_cluster(Node) ->
    gen_server:cast(?SERVER, {left_cluster, Node}).

notify_left_cluster(Node) ->
    left_cluster(Node),
    cluster_multicall(left_cluster, [Node]),
    ok.

node_up(Node, IsDiscNode) ->
     gen_server:cast(?SERVER, {node_up, Node, IsDiscNode}).

notify_node_up() ->
    Nodes = cluster_multicall(node_up, [node(), rabbit_mnesia:is_disc_node()]),
    %% register other active rabbits with this rabbit
    [ node_up(N, ordsets:is_element(N, rabbit_mnesia:clustered_disc_nodes())) ||
        N <- Nodes ],
    ok.

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, no_state}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

%% Note: when updating the status file, we can't simply write the mnesia
%% information since the message can (and will) overtake the mnesia propagation.
handle_cast({node_up, Node, IsDiscNode}, State) ->
    case is_already_monitored({rabbit, Node}) of
        true  -> {noreply, State};
        false -> rabbit_log:info("rabbit on node ~p up~n", [Node]),
                 {AllNodes, DiscNodes, RunningNodes} = read_cluster_status(),
                 write_cluster_status({ordsets:add_element(Node, AllNodes),
                                       case IsDiscNode of
                                           true  -> ordsets:add_element(
                                                      Node, DiscNodes);
                                           false -> DiscNodes
                                       end,
                                       ordsets:add_element(Node, RunningNodes)}),
                 erlang:monitor(process, {rabbit, Node}),
                 ok = handle_live_rabbit(Node),
                 {noreply, State}
    end;
handle_cast({joined_cluster, Node, IsDiscNode}, State) ->
    {AllNodes, DiscNodes, RunningNodes} = read_cluster_status(),
    write_cluster_status({ordsets:add_element(Node, AllNodes),
                          case IsDiscNode of
                              true  -> ordsets:add_element(Node,
                                                           DiscNodes);
                              false -> DiscNodes
                          end,
                          RunningNodes}),
    {noreply, State};
handle_cast({left_cluster, Node}, State) ->
    {AllNodes, DiscNodes, RunningNodes} = read_cluster_status(),
    write_cluster_status({ordsets:del_element(Node, AllNodes),
                          ordsets:del_element(Node, DiscNodes),
                          ordsets:del_element(Node, RunningNodes)}),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MRef, process, {rabbit, Node}, _Reason}, State) ->
    rabbit_log:info("rabbit on node ~p down~n", [Node]),
    {AllNodes, DiscNodes, RunningNodes} = read_cluster_status(),
    write_cluster_status({AllNodes, DiscNodes,
                          ordsets:del_element(Node, RunningNodes)}),
    ok = handle_dead_rabbit(Node),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
%% Functions that call the module specific hooks when nodes go up/down
%%----------------------------------------------------------------------------

%% TODO: This may turn out to be a performance hog when there are lots
%% of nodes.  We really only need to execute some of these statements
%% on *one* node, rather than all of them.
handle_dead_rabbit(Node) ->
    ok = rabbit_networking:on_node_down(Node),
    ok = rabbit_amqqueue:on_node_down(Node),
    ok = rabbit_alarm:on_node_down(Node),
    ok = rabbit_mnesia:on_node_down(Node).

handle_live_rabbit(Node) ->
    ok = rabbit_alarm:on_node_up(Node),
    ok = rabbit_mnesia:on_node_up(Node).

%%--------------------------------------------------------------------
%% Internal utils
%%--------------------------------------------------------------------

cluster_multicall(Fun, Args) ->
    Node = node(),
    Nodes = rabbit_mnesia:running_clustered_nodes() -- [Node],
    %% notify other rabbits of this cluster
    case rpc:multicall(Nodes, rabbit_node_monitor, Fun, Args,
                       ?RABBIT_UP_RPC_TIMEOUT) of
        {_, [] } -> ok;
        {_, Bad} -> rabbit_log:info("failed to contact nodes ~p~n", [Bad])
    end,
    Nodes.

is_already_monitored(Item) ->
    {monitors, Monitors} = process_info(self(), monitors),
    lists:any(fun ({_, Item1}) when Item =:= Item1 -> true;
                  (_)                              -> false
              end, Monitors).

legacy_cluster_nodes(Nodes) ->
    %% We get all the info that we can, including the nodes from mnesia, which
    %% will be there if the node is a disc node (empty list otherwise)
    lists:usort(Nodes ++ mnesia:system_info(db_nodes)).

legacy_should_be_disc_node(DiscNodes) ->
    DiscNodes == [] orelse lists:member(node(), DiscNodes).
