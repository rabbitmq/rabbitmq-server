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

-export([prepare_cluster_status_file/0,
         write_cluster_status_file/1,
         read_cluster_status_file/0,
         update_cluster_status_file/0,
         reset_cluster_status_file/0,

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

-spec(prepare_cluster_status_file/0 :: () -> 'ok').
-spec(write_cluster_status_file/1 :: (rabbit_mnesia:cluster_status())
                                  -> 'ok').
-spec(read_cluster_status_file/0 :: () -> rabbit_mnesia:cluster_status()).
-spec(update_cluster_status_file/0 :: () -> 'ok').
-spec(reset_cluster_status_file/0 :: () -> 'ok').

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

%% The cluster node status file contains all we need to know about the cluster:
%%
%%   * All the clustered nodes
%%   * The disc nodes
%%   * The running nodes.
%%
%% If the current node is a disc node it will be included in the disc nodes
%% list.
%%
%% We strive to keep the file up to date and we rely on this assumption in
%% various situations. Obviously when mnesia is offline the information we have
%% will be outdated, but it can't be otherwise.

cluster_status_file_filename() ->
    rabbit_mnesia:dir() ++ "/cluster_nodes.config".

prepare_cluster_status_file() ->
    NotPresent =
        fun (AllNodes0, WantDiscNode) ->
            ThisNode = [node()],

            RunningNodes0 = legacy_read_previously_running_nodes(),
            legacy_delete_previously_running_nodes(),

            RunningNodes = lists:usort(RunningNodes0 ++ ThisNode),
            AllNodes =
                lists:usort(AllNodes0 ++ RunningNodes),
            DiscNodes = case WantDiscNode of
                            true  -> ThisNode;
                            false -> []
                        end,

            ok = write_cluster_status_file({AllNodes, DiscNodes, RunningNodes})
        end,
    case try_read_cluster_status_file() of
        {ok, _} ->
            ok;
        {error, {invalid_term, _, [AllNodes]}} ->
            %% Legacy file
            NotPresent(AllNodes, legacy_should_be_disc_node(AllNodes));
        {error, {cannot_read_file, _, enoent}} ->
            {ok, {AllNodes, WantDiscNode}} =
                application:get_env(rabbit, cluster_nodes),
            NotPresent(AllNodes, WantDiscNode)
    end.


write_cluster_status_file(Status) ->
    FileName = cluster_status_file_filename(),
    case rabbit_file:write_term_file(FileName, [Status]) of
        ok -> ok;
        {error, Reason} ->
            throw({error, {cannot_write_cluster_status_file,
                           FileName, Reason}})
    end.

try_read_cluster_status_file() ->
    FileName = cluster_status_file_filename(),
    case rabbit_file:read_term_file(FileName) of
        {ok, [{_, _, _} = Status]} ->
            {ok, Status};
        {ok, Term} ->
            {error, {invalid_term, FileName, Term}};
        {error, Reason} ->
            {error, {cannot_read_file, FileName, Reason}}
    end.

read_cluster_status_file() ->
    case try_read_cluster_status_file() of
        {ok, Status} ->
            Status;
        {error, Reason} ->
            throw({error, {cannot_read_cluster_status_file, Reason}})
    end.

update_cluster_status_file() ->
    {ok, Status} = rabbit_mnesia:cluster_status_from_mnesia(),
    write_cluster_status_file(Status).

reset_cluster_status_file() ->
    write_cluster_status_file({[node()], [node()], [node()]}).

%%----------------------------------------------------------------------------
%% Cluster notifications
%%----------------------------------------------------------------------------

joined_cluster(Node, IsDiscNode) ->
    gen_server:cast(rabbit_node_monitor, {rabbit_join, Node, IsDiscNode}).

notify_joined_cluster() ->
    cluster_multicall(joined_cluster, [node(), rabbit_mnesia:is_disc_node()]),
    ok.

left_cluster(Node) ->
    gen_server:cast(rabbit_node_monitor, {left_cluster, Node}).

notify_left_cluster(Node) ->
    left_cluster(Node),
    cluster_multicall(left_cluster, [Node]),
    ok.

node_up(Node, IsDiscNode) ->
     gen_server:cast(rabbit_node_monitor, {node_up, Node, IsDiscNode}).

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
                 {ok, {AllNodes, DiscNodes, RunningNodes}} =
                     rabbit_mnesia:cluster_status_from_mnesia(),
                 write_cluster_status_file(
                   {ordsets:add_element(Node, AllNodes),
                    case IsDiscNode of
                        true  -> ordsets:add_element(Node, DiscNodes);
                        false -> DiscNodes
                    end,
                    ordsets:add_element(Node, RunningNodes)}),
                 erlang:monitor(process, {rabbit, Node}),
                 ok = handle_live_rabbit(Node),
                 {noreply, State}
    end;
handle_cast({joined_cluster, Node, IsDiscNode}, State) ->
    {ok, {AllNodes, DiscNodes, RunningNodes}} =
        rabbit_mnesia:cluster_status_from_mnesia(),
    write_cluster_status_file({ordsets:add_element(Node, AllNodes),
                               case IsDiscNode of
                                    true  -> ordsets:add_element(Node,
                                                                 DiscNodes);
                                    false -> DiscNodes
                                end,
                               RunningNodes}),
    {noreply, State};
handle_cast({left_cluster, Node}, State) ->
    {ok, {AllNodes, DiscNodes, RunningNodes}} =
        rabbit_mnesia:cluster_status_from_mnesia(),
    write_cluster_status_file({ordsets:del_element(Node, AllNodes),
                               ordsets:del_element(Node, DiscNodes),
                               ordsets:del_element(Node, RunningNodes)}),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MRef, process, {rabbit, Node}, _Reason}, State) ->
    rabbit_log:info("rabbit on node ~p down~n", [Node]),
    {ok, {AllNodes, DiscNodes, RunningNodes}} =
        rabbit_mnesia:cluster_status_from_mnesia(),
    write_cluster_status_file({AllNodes, DiscNodes,
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
    ok = rabbit_alarm:on_node_down(Node).

handle_live_rabbit(Node) ->
    ok = rabbit_alarm:on_node_up(Node),
    ok = rabbit_mnesia:on_node_down(Node).

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

legacy_should_be_disc_node(DiscNodes) ->
    DiscNodes == [] orelse lists:member(node(), DiscNodes).

%%--------------------------------------------------------------------
%% Legacy functions related to the "running nodes" file
%%--------------------------------------------------------------------

legacy_running_nodes_filename() ->
    filename:join(rabbit_mnesia:dir(), "nodes_running_at_shutdown").

legacy_read_previously_running_nodes() ->
    FileName = legacy_running_nodes_filename(),
    case rabbit_file:read_term_file(FileName) of
        {ok, [Nodes]}   -> Nodes;
        {error, enoent} -> [];
        {error, Reason} -> throw({error, {cannot_read_previous_nodes_file,
                                          FileName, Reason}})
    end.

legacy_delete_previously_running_nodes() ->
    FileName = legacy_running_nodes_filename(),
    case file:delete(FileName) of
        ok              -> ok;
        {error, enoent} -> ok;
        {error, Reason} -> throw({error, {cannot_delete_previous_nodes_file,
                                          FileName, Reason}})
    end.
