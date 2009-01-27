%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_networking).

-export([start/0, start_tcp_listener/2, stop_tcp_listener/2,
         on_node_down/1, active_listeners/0, node_listeners/1,
         connections/0, connection_info/1, connection_info/2,
         connection_info_all/0, connection_info_all/1]).
%%used by TCP-based transports, e.g. STOMP adapter
-export([check_tcp_listener_address/3]).

-export([tcp_listener_started/2, tcp_listener_stopped/2, start_client/1]).

-include("rabbit.hrl").
-include_lib("kernel/include/inet.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(host() :: ip_address() | string() | atom()).
-type(connection() :: pid()).

-spec(start/0 :: () -> 'ok').
-spec(start_tcp_listener/2 :: (host(), ip_port()) -> 'ok').
-spec(stop_tcp_listener/2 :: (host(), ip_port()) -> 'ok').
-spec(active_listeners/0 :: () -> [listener()]).
-spec(node_listeners/1 :: (erlang_node()) -> [listener()]).
-spec(connections/0 :: () -> [connection()]).
-spec(connection_info/1 :: (connection()) -> [info()]).
-spec(connection_info/2 :: (connection(), [info_key()]) -> [info()]).
-spec(connection_info_all/0 :: () -> [[info()]]).
-spec(connection_info_all/1 :: ([info_key()]) -> [[info()]]).
-spec(on_node_down/1 :: (erlang_node()) -> 'ok').
-spec(check_tcp_listener_address/3 :: (atom(), host(), ip_port()) ->
             {ip_address(), atom()}).

-endif.

%%----------------------------------------------------------------------------

start() ->
    {ok,_} = supervisor:start_child(
               rabbit_sup,
               {rabbit_tcp_client_sup,
                {tcp_client_sup, start_link,
                 [{local, rabbit_tcp_client_sup},
                  {rabbit_reader,start_link,[]}]},
                transient, infinity, supervisor, [tcp_client_sup]}),
    ok.

check_tcp_listener_address(NamePrefix, Host, Port) ->
    IPAddress =
        case inet:getaddr(Host, inet) of
            {ok, IPAddress1} -> IPAddress1;
            {error, Reason} ->
                error_logger:error_msg("invalid host ~p - ~p~n",
                                       [Host, Reason]),
                throw({error, {invalid_host, Host, Reason}})
        end,
    if is_integer(Port) andalso (Port >= 0) andalso (Port =< 65535) -> ok;
       true -> error_logger:error_msg("invalid port ~p - not 0..65535~n",
                                      [Port]),
               throw({error, invalid_port, Port})
    end,
    Name = rabbit_misc:tcp_name(NamePrefix, IPAddress, Port),
    {IPAddress, Name}.

start_tcp_listener(Host, Port) ->
    {IPAddress, Name} = check_tcp_listener_address(rabbit_tcp_listener_sup, Host, Port),
    {ok,_} = supervisor:start_child(
               rabbit_sup,
               {Name,
                {tcp_listener_sup, start_link,
                 [IPAddress, Port,
                  [binary,
                   {packet, raw}, % no packaging
                   {reuseaddr, true}, % allow rebind without waiting
                   %% {nodelay, true}, % TCP_NODELAY - disable Nagle's alg.
                   %% {delay_send, true},
                   {exit_on_close, false}],
                  {?MODULE, tcp_listener_started, []},
                  {?MODULE, tcp_listener_stopped, []},
                  {?MODULE, start_client, []}]},
                transient, infinity, supervisor, [tcp_listener_sup]}),
    ok.

stop_tcp_listener(Host, Port) ->
    {ok, IPAddress} = inet:getaddr(Host, inet),
    Name = rabbit_misc:tcp_name(rabbit_tcp_listener_sup, IPAddress, Port),
    ok = supervisor:terminate_child(rabbit_sup, Name),
    ok = supervisor:delete_child(rabbit_sup, Name),
    ok.

tcp_listener_started(IPAddress, Port) ->
    ok = mnesia:dirty_write(
           rabbit_listener,
           #listener{node = node(),
                     protocol = tcp,
                     host = tcp_host(IPAddress),
                     port = Port}).

tcp_listener_stopped(IPAddress, Port) ->
    ok = mnesia:dirty_delete_object(
           rabbit_listener,
           #listener{node = node(),
                     protocol = tcp,
                     host = tcp_host(IPAddress),
                     port = Port}).

active_listeners() ->
    rabbit_misc:dirty_read_all(rabbit_listener).

node_listeners(Node) ->
    mnesia:dirty_read(rabbit_listener, Node).

on_node_down(Node) ->
    ok = mnesia:dirty_delete(rabbit_listener, Node).

start_client(Sock) ->
    {ok, Child} = supervisor:start_child(rabbit_tcp_client_sup, []),
    ok = gen_tcp:controlling_process(Sock, Child),
    Child ! {go, Sock},
    Child.

connections() ->
    [Pid || {_, Pid, _, _} <- supervisor:which_children(
                                rabbit_tcp_client_sup)].

connection_info(Pid) -> rabbit_reader:info(Pid).
connection_info(Pid, Items) -> rabbit_reader:info(Pid, Items).

connection_info_all() -> cmap(fun (Q) -> connection_info(Q) end).
connection_info_all(Items) -> cmap(fun (Q) -> connection_info(Q, Items) end).

%%--------------------------------------------------------------------

tcp_host({0,0,0,0}) ->
    {ok, Hostname} = inet:gethostname(),
    case inet:gethostbyname(Hostname) of
        {ok, #hostent{h_name = Name}} -> Name;
        {error, _Reason} -> Hostname
    end;
tcp_host(IPAddress) ->
    case inet:gethostbyaddr(IPAddress) of
        {ok, #hostent{h_name = Name}} -> Name;
        {error, _Reason} -> inet_parse:ntoa(IPAddress)
    end.

cmap(F) -> rabbit_misc:filter_exit_map(F, connections()).
