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
%% Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.
%%
-module(inet_proxy_dist).

%% Transitional step until we can require Erlang/OTP 21 and
%% use the now recommended try/catch syntax for obtaining the stack trace.
-compile(nowarn_deprecated_function).

%% A distribution plugin that uses the usual inet_tcp_dist but allows
%% insertion of a proxy at the receiving end.

%% inet_*_dist "behaviour"
-export([listen/1, accept/1, accept_connection/5,
	 setup/5, close/1, select/1, is_node_name/1]).

%% For copypasta from inet_tcp_dist
-export([do_setup/6]).
-import(error_logger,[error_msg/2]).

-define(REAL, inet_tcp_dist).

%%----------------------------------------------------------------------------

listen(Name)       -> ?REAL:listen(Name).
select(Node)       -> ?REAL:select(Node).
accept(Listen)     -> ?REAL:accept(Listen).
close(Socket)      -> ?REAL:close(Socket).
is_node_name(Node) -> ?REAL:is_node_name(Node).

accept_connection(AcceptPid, Socket, MyNode, Allowed, SetupTime) ->
    ?REAL:accept_connection(AcceptPid, Socket, MyNode, Allowed, SetupTime).

%% This is copied from inet_tcp_dist, in order to change the
%% output of erl_epmd:port_please/2.

-include_lib("kernel/include/net_address.hrl").
-include_lib("kernel/include/dist_util.hrl").

setup(Node, Type, MyNode, LongOrShortNames,SetupTime) ->
    spawn_opt(?MODULE, do_setup, 
	      [self(), Node, Type, MyNode, LongOrShortNames, SetupTime],
	      [link, {priority, max}]).

do_setup(Kernel, Node, Type, MyNode, LongOrShortNames,SetupTime) ->
    ?trace("~p~n",[{inet_tcp_dist,self(),setup,Node}]),
    [Name, Address] = splitnode(Node, LongOrShortNames),
    case inet:getaddr(Address, inet) of
	{ok, Ip} ->
	    Timer = dist_util:start_timer(SetupTime),
	    case erl_epmd:port_please(Name, Ip) of
		{port, TcpPort, Version} ->
		    ?trace("port_please(~p) -> version ~p~n", 
			   [Node,Version]),
		    dist_util:reset_timer(Timer),
                    %% Modification START
                    Ret = application:get_env(kernel,
                      dist_and_proxy_ports_map),
                    PortsMap = case Ret of
                        {ok, M}   -> M;
                        undefined -> []
                    end,
                    ProxyPort = case inet_tcp_proxy:is_enabled() of
                        true  -> P = proplists:get_value(TcpPort, PortsMap, TcpPort),
                                 error_logger:info_msg(
                                   "Using inet_tcp_proxy to connect to ~s (remote port changed from ~b to ~b)~n",
                                   [Node, TcpPort, P]),
                                 P;
                        false -> TcpPort
                    end,
		    case inet_tcp:connect(Ip, ProxyPort,
					  [{active, false},
					   {packet,2}]) of
			{ok, Socket} ->
                            {ok, {_, SrcPort}} = inet:sockname(Socket),
                            ok = inet_tcp_proxy_manager:register(
                                   node(), Node, SrcPort, TcpPort, ProxyPort),
                    %% Modification END
			    HSData = #hs_data{
			      kernel_pid = Kernel,
			      other_node = Node,
			      this_node = MyNode,
			      socket = Socket,
			      timer = Timer,
			      this_flags = 0,
			      other_version = Version,
			      f_send = fun inet_tcp:send/2,
			      f_recv = fun inet_tcp:recv/3,
			      f_setopts_pre_nodeup = 
			      fun(S) ->
				      inet:setopts
					(S, 
					 [{active, false},
					  {packet, 4},
					  nodelay()])
			      end,
			      f_setopts_post_nodeup = 
			      fun(S) ->
				      inet:setopts
					(S, 
					 [{active, true},
					  {deliver, port},
					  {packet, 4},
					  nodelay()])
			      end,
			      f_getll = fun inet:getll/1,
			      f_address = 
			      fun(_,_) ->
				      #net_address{
				   address = {Ip,TcpPort},
				   host = Address,
				   protocol = tcp,
				   family = inet}
			      end,
			      mf_tick = fun tick/1,
			      mf_getstat = fun inet_tcp_dist:getstat/1,
			      request_type = Type
			     },
			    dist_util:handshake_we_started(HSData);
			R ->
                            io:format("~p failed! ~p~n", [node(), R]),
			    %% Other Node may have closed since 
			    %% port_please !
			    ?trace("other node (~p) "
				   "closed since port_please.~n", 
				   [Node]),
			    ?shutdown(Node)
		    end;
		_ ->
		    ?trace("port_please (~p) "
			   "failed.~n", [Node]),
		    ?shutdown(Node)
	    end;
	_Other ->
	    ?trace("inet_getaddr(~p) "
		   "failed (~p).~n", [Node,_Other]),
	    ?shutdown(Node)
    end.

%% If Node is illegal terminate the connection setup!!
splitnode(Node, LongOrShortNames) ->
    case split_node(atom_to_list(Node), $@, []) of
	[Name|Tail] when Tail =/= [] ->
	    Host = lists:append(Tail),
	    case split_node(Host, $., []) of
		[_] when LongOrShortNames =:= longnames ->
		    error_msg("** System running to use "
			      "fully qualified "
			      "hostnames **~n"
			      "** Hostname ~s is illegal **~n",
			      [Host]),
		    ?shutdown(Node);
		L when length(L) > 1, LongOrShortNames =:= shortnames ->
		    error_msg("** System NOT running to use fully qualified "
			      "hostnames **~n"
			      "** Hostname ~s is illegal **~n",
			      [Host]),
		    ?shutdown(Node);
		_ ->
		    [Name, Host]
	    end;
	[_] ->
	    error_msg("** Nodename ~p illegal, no '@' character **~n",
		      [Node]),
	    ?shutdown(Node);
	_ ->
	    error_msg("** Nodename ~p illegal **~n", [Node]),
	    ?shutdown(Node)
    end.

split_node([Chr|T], Chr, Ack) -> [lists:reverse(Ack)|split_node(T, Chr, [])];
split_node([H|T], Chr, Ack)   -> split_node(T, Chr, [H|Ack]);
split_node([], _, Ack)        -> [lists:reverse(Ack)].

%% we may not always want the nodelay behaviour
%% for performance reasons

nodelay() ->
    case application:get_env(kernel, dist_nodelay) of
	undefined ->
	    {nodelay, true};
	{ok, true} ->
	    {nodelay, true};
	{ok, false} ->
	    {nodelay, false};
	_ ->
	    {nodelay, true}
    end.

tick(Socket) ->
    case inet_tcp:send(Socket, [], [force]) of
        {error, closed} ->
            self() ! {tcp_closed, Socket},
            {error, closed};
        R ->
            R
    end.
