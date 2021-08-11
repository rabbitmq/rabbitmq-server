-module(rabbit_prelaunch_dist).

-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([setup/1]).

setup(#{nodename := Node, nodename_type := NameType} = Context) ->
    ?LOG_DEBUG(
       "~n== Erlang distribution ==", [],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ?LOG_DEBUG(
       "Rqeuested node name: ~s (type: ~s)",
       [Node, NameType],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    case node() of
        nonode@nohost ->
            ok = rabbit_nodes_common:ensure_epmd(),
            ok = dist_port_range_check(Context),
            ok = dist_port_use_check(Context),
            ok = duplicate_node_check(Context),

            ok = do_setup(Context);
        Node ->
            ?LOG_DEBUG(
              "Erlang distribution already running", [],
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            ok;
        Unexpected ->
            throw({error, {erlang_dist_running_with_unexpected_nodename,
                           Unexpected, Node}})
    end,
    ok.

do_setup(#{nodename := Node,
           nodename_type := NameType,
           var_origins := Origins} = Config) ->
    ?LOG_DEBUG(
       "Starting Erlang distribution",
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    case application:get_env(kernel, net_ticktime) of
        {ok, Ticktime} when is_integer(Ticktime) andalso Ticktime >= 1 ->
            %% The value passed to net_kernel:start/1 is the
            %% "minimum transition traffic interval" as defined in
            %% net_kernel:set_net_ticktime/1.
            MTTI = Ticktime * 1000 div 4,
            {ok, _} = net_kernel:start([Node, NameType, MTTI]),
            ok;
        _ ->
            {ok, _} = net_kernel:start([Node, NameType]),
            ok
    end,

    %% Override the Erlang cookie if one was set in the environment.
    case maps:get(erlang_cookie, Origins, default) of
        environment ->
            ?LOG_WARNING(
               "Overriding Erlang cookie using the value set in the environment",
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            Cookie = maps:get(erlang_cookie, Config),
            ?assert(is_atom(Cookie)),
            true = erlang:set_cookie(node(), Cookie);
        _ ->
            ok
    end,
    ok.

%% Check whether a node with the same name is already running
duplicate_node_check(#{split_nodename := {NodeName, NodeHost}}) ->
    ?LOG_DEBUG(
      "Checking if node name ~s is already used", [NodeName],
      #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    PrelaunchName = rabbit_nodes_common:make(
                      {NodeName ++ "_prelaunch_" ++ os:getpid(),
                       "localhost"}),
    {ok, _} = net_kernel:start([PrelaunchName, shortnames]),
    case rabbit_nodes_common:names(NodeHost) of
        {ok, NamePorts}  ->
            case proplists:is_defined(NodeName, NamePorts) of
                true ->
                    throw({error, {duplicate_node_name, NodeName, NodeHost}});
                false ->
                    ok = net_kernel:stop(),
                    ok
            end;
        {error, EpmdReason} ->
            throw({error, {epmd_error, NodeHost, EpmdReason}})
    end.

dist_port_range_check(#{erlang_dist_tcp_port := DistTcpPort}) ->
    ?LOG_DEBUG(
      "Checking if TCP port ~b is valid", [DistTcpPort],
      #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    case DistTcpPort of
        _ when DistTcpPort < 1 orelse DistTcpPort > 65535 ->
            throw({error, {invalid_dist_port_range, DistTcpPort}});
        _ ->
            ok
    end.

dist_port_use_check(#{split_nodename := {_, NodeHost},
                      erlang_dist_tcp_port := DistTcpPort}) ->
    ?LOG_DEBUG(
       "Checking if TCP port ~b is available", [DistTcpPort],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    dist_port_use_check_ipv4(NodeHost, DistTcpPort).

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
    {ok, Names} = rabbit_nodes_common:names(Host),
    case [N || {N, P} <- Names, P =:= Port] of
        [] ->
            throw({error, {dist_port_already_used, Port, not_erlang, Host}});
        [Name] ->
            throw({error, {dist_port_already_used, Port, Name, Host}})
    end.
