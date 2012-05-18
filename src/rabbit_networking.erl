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

-module(rabbit_networking).

-export([boot/0, start/0, start_tcp_listener/1, start_ssl_listener/2,
         stop_tcp_listener/1, on_node_down/1, active_listeners/0,
         node_listeners/1, connections/0, connection_info_keys/0,
         connection_info/1, connection_info/2,
         connection_info_all/0, connection_info_all/1,
         close_connection/2, force_connection_event_refresh/0]).

%%used by TCP-based transports, e.g. STOMP adapter
-export([tcp_listener_addresses/1, tcp_listener_spec/6,
         ensure_ssl/0, ssl_transform_fun/1]).

-export([tcp_listener_started/3, tcp_listener_stopped/3,
         start_client/1, start_ssl_client/2]).

%% Internal
-export([connections_local/0]).

-include("rabbit.hrl").
-include_lib("kernel/include/inet.hrl").

-define(SSL_TIMEOUT, 5). %% seconds

-define(FIRST_TEST_BIND_PORT, 10000).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([ip_port/0, hostname/0]).

-type(hostname() :: inet:hostname()).
-type(ip_port() :: inet:port_number()).

-type(family() :: atom()).
-type(listener_config() :: ip_port() |
                           {hostname(), ip_port()} |
                           {hostname(), ip_port(), family()}).
-type(address() :: {inet:ip_address(), ip_port(), family()}).
-type(name_prefix() :: atom()).
-type(protocol() :: atom()).
-type(label() :: string()).

-spec(start/0 :: () -> 'ok').
-spec(start_tcp_listener/1 :: (listener_config()) -> 'ok').
-spec(start_ssl_listener/2 ::
        (listener_config(), rabbit_types:infos()) -> 'ok').
-spec(stop_tcp_listener/1 :: (listener_config()) -> 'ok').
-spec(active_listeners/0 :: () -> [rabbit_types:listener()]).
-spec(node_listeners/1 :: (node()) -> [rabbit_types:listener()]).
-spec(connections/0 :: () -> [rabbit_types:connection()]).
-spec(connections_local/0 :: () -> [rabbit_types:connection()]).
-spec(connection_info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(connection_info/1 ::
        (rabbit_types:connection()) -> rabbit_types:infos()).
-spec(connection_info/2 ::
        (rabbit_types:connection(), rabbit_types:info_keys())
        -> rabbit_types:infos()).
-spec(connection_info_all/0 :: () -> [rabbit_types:infos()]).
-spec(connection_info_all/1 ::
        (rabbit_types:info_keys()) -> [rabbit_types:infos()]).
-spec(close_connection/2 :: (pid(), string()) -> 'ok').
-spec(force_connection_event_refresh/0 :: () -> 'ok').

-spec(on_node_down/1 :: (node()) -> 'ok').
-spec(tcp_listener_addresses/1 :: (listener_config()) -> [address()]).
-spec(tcp_listener_spec/6 ::
        (name_prefix(), address(), [gen_tcp:listen_option()], protocol(),
         label(), rabbit_types:mfargs()) -> supervisor:child_spec()).
-spec(ensure_ssl/0 :: () -> rabbit_types:infos()).
-spec(ssl_transform_fun/1 ::
        (rabbit_types:infos())
        -> fun ((rabbit_net:socket())
                -> rabbit_types:ok_or_error(#ssl_socket{}))).

-spec(boot/0 :: () -> 'ok').
-spec(start_client/1 ::
	(port() | #ssl_socket{ssl::{'sslsocket',_,_}}) ->
			     atom() | pid() | port() | {atom(),atom()}).
-spec(start_ssl_client/2 ::
	(_,port() | #ssl_socket{ssl::{'sslsocket',_,_}}) ->
				 atom() | pid() | port() | {atom(),atom()}).
-spec(tcp_listener_started/3 ::
	(_,
         string() |
	 {byte(),byte(),byte(),byte()} |
	 {char(),char(),char(),char(),char(),char(),char(),char()},
	 _) ->
				     'ok').
-spec(tcp_listener_stopped/3 ::
	(_,
         string() |
	 {byte(),byte(),byte(),byte()} |
	 {char(),char(),char(),char(),char(),char(),char(),char()},
	 _) ->
				     'ok').

-endif.

%%----------------------------------------------------------------------------

boot() ->
    ok = start(),
    ok = boot_tcp(),
    ok = boot_ssl().

boot_tcp() ->
    {ok, TcpListeners} = application:get_env(tcp_listeners),
    [ok = start_tcp_listener(Listener) || Listener <- TcpListeners],
    ok.

boot_ssl() ->
    case application:get_env(ssl_listeners) of
        {ok, []} ->
            ok;
        {ok, SslListeners} ->
            [start_ssl_listener(Listener, ensure_ssl())
             || Listener <- SslListeners],
            ok
    end.

start() -> rabbit_sup:start_supervisor_child(
             rabbit_tcp_client_sup, rabbit_client_sup,
             [{local, rabbit_tcp_client_sup},
              {rabbit_connection_sup,start_link,[]}]).

ensure_ssl() ->
    ok = rabbit_misc:start_applications([crypto, public_key, ssl]),
    {ok, SslOptsConfig} = application:get_env(rabbit, ssl_options),

    % unknown_ca errors are silently ignored prior to R14B unless we
    % supply this verify_fun - remove when at least R14B is required
    case proplists:get_value(verify, SslOptsConfig, verify_none) of
        verify_none -> SslOptsConfig;
        verify_peer -> [{verify_fun, fun([])    -> true;
                                        ([_|_]) -> false
                                     end}
                        | SslOptsConfig]
    end.

ssl_transform_fun(SslOpts) ->
    fun (Sock) ->
            case catch ssl:ssl_accept(Sock, SslOpts, ?SSL_TIMEOUT * 1000) of
                {ok, SslSock} ->
                    {ok, #ssl_socket{tcp = Sock, ssl = SslSock}};
                {error, Reason} ->
                    {error, {ssl_upgrade_error, Reason}};
                {'EXIT', Reason} ->
                    {error, {ssl_upgrade_failure, Reason}}
            end
    end.

tcp_listener_addresses(Port) when is_integer(Port) ->
    tcp_listener_addresses_auto(Port);
tcp_listener_addresses({"auto", Port}) ->
    %% Variant to prevent lots of hacking around in bash and batch files
    tcp_listener_addresses_auto(Port);
tcp_listener_addresses({Host, Port}) ->
    %% auto: determine family IPv4 / IPv6 after converting to IP address
    tcp_listener_addresses({Host, Port, auto});
tcp_listener_addresses({Host, Port, Family0})
  when is_integer(Port) andalso (Port >= 0) andalso (Port =< 65535) ->
    [{IPAddress, Port, Family} ||
        {IPAddress, Family} <- getaddr(Host, Family0)];
tcp_listener_addresses({_Host, Port, _Family0}) ->
    error_logger:error_msg("invalid port ~p - not 0..65535~n", [Port]),
    throw({error, {invalid_port, Port}}).

tcp_listener_addresses_auto(Port) ->
    lists:append([tcp_listener_addresses(Listener) ||
                     Listener <- port_to_listeners(Port)]).

tcp_listener_spec(NamePrefix, {IPAddress, Port, Family}, SocketOpts,
                  Protocol, Label, OnConnect) ->
    {rabbit_misc:tcp_name(NamePrefix, IPAddress, Port),
     {tcp_listener_sup, start_link,
      [IPAddress, Port, [Family | SocketOpts],
       {?MODULE, tcp_listener_started, [Protocol]},
       {?MODULE, tcp_listener_stopped, [Protocol]},
       OnConnect, Label]},
     transient, infinity, supervisor, [tcp_listener_sup]}.

start_tcp_listener(Listener) ->
    start_listener(Listener, amqp, "TCP Listener",
                   {?MODULE, start_client, []}).

start_ssl_listener(Listener, SslOpts) ->
    start_listener(Listener, 'amqp/ssl', "SSL Listener",
                   {?MODULE, start_ssl_client, [SslOpts]}).

start_listener(Listener, Protocol, Label, OnConnect) ->
    [start_listener0(Address, Protocol, Label, OnConnect) ||
        Address <- tcp_listener_addresses(Listener)],
    ok.

start_listener0(Address, Protocol, Label, OnConnect) ->
    Spec = tcp_listener_spec(rabbit_tcp_listener_sup, Address, tcp_opts(),
                             Protocol, Label, OnConnect),
    case supervisor:start_child(rabbit_sup, Spec) of
        {ok, _}                -> ok;
        {error, {shutdown, _}} -> {IPAddress, Port, _Family} = Address,
                                  exit({could_not_start_tcp_listener,
                                        {rabbit_misc:ntoa(IPAddress), Port}})
    end.

stop_tcp_listener(Listener) ->
    [stop_tcp_listener0(Address) ||
        Address <- tcp_listener_addresses(Listener)],
    ok.

stop_tcp_listener0({IPAddress, Port, _Family}) ->
    Name = rabbit_misc:tcp_name(rabbit_tcp_listener_sup, IPAddress, Port),
    ok = supervisor:terminate_child(rabbit_sup, Name),
    ok = supervisor:delete_child(rabbit_sup, Name).

tcp_listener_started(Protocol, IPAddress, Port) ->
    %% We need the ip to distinguish e.g. 0.0.0.0 and 127.0.0.1
    %% We need the host so we can distinguish multiple instances of the above
    %% in a cluster.
    ok = mnesia:dirty_write(
           rabbit_listener,
           #listener{node = node(),
                     protocol = Protocol,
                     host = tcp_host(IPAddress),
                     ip_address = IPAddress,
                     port = Port}).

tcp_listener_stopped(Protocol, IPAddress, Port) ->
    ok = mnesia:dirty_delete_object(
           rabbit_listener,
           #listener{node = node(),
                     protocol = Protocol,
                     host = tcp_host(IPAddress),
                     ip_address = IPAddress,
                     port = Port}).

active_listeners() ->
    rabbit_misc:dirty_read_all(rabbit_listener).

node_listeners(Node) ->
    mnesia:dirty_read(rabbit_listener, Node).

on_node_down(Node) ->
    ok = mnesia:dirty_delete(rabbit_listener, Node).

start_client(Sock, SockTransform) ->
    {ok, _Child, Reader} = supervisor:start_child(rabbit_tcp_client_sup, []),
    ok = rabbit_net:controlling_process(Sock, Reader),
    Reader ! {go, Sock, SockTransform},

    %% In the event that somebody floods us with connections, the
    %% reader processes can spew log events at error_logger faster
    %% than it can keep up, causing its mailbox to grow unbounded
    %% until we eat all the memory available and crash. So here is a
    %% meaningless synchronous call to the underlying gen_event
    %% mechanism. When it returns the mailbox is drained, and we
    %% return to our caller to accept more connetions.
    gen_event:which_handlers(error_logger),

    Reader.

start_client(Sock) ->
    start_client(Sock, fun (S) -> {ok, S} end).

start_ssl_client(SslOpts, Sock) ->
    start_client(Sock, ssl_transform_fun(SslOpts)).

connections() ->
    rabbit_misc:append_rpc_all_nodes(rabbit_mnesia:running_clustered_nodes(),
                                     rabbit_networking, connections_local, []).

connections_local() ->
    [Reader ||
        {_, ConnSup, supervisor, _}
            <- supervisor:which_children(rabbit_tcp_client_sup),
        Reader <- [try
                       rabbit_connection_sup:reader(ConnSup)
                   catch exit:{noproc, _} ->
                           noproc
                   end],
        Reader =/= noproc].

connection_info_keys() -> rabbit_reader:info_keys().

connection_info(Pid) -> rabbit_reader:info(Pid).
connection_info(Pid, Items) -> rabbit_reader:info(Pid, Items).

connection_info_all() -> cmap(fun (Q) -> connection_info(Q) end).
connection_info_all(Items) -> cmap(fun (Q) -> connection_info(Q, Items) end).

close_connection(Pid, Explanation) ->
    rabbit_log:info("Closing connection ~p because ~p~n", [Pid, Explanation]),
    case lists:member(Pid, connections()) of
        true  -> rabbit_reader:shutdown(Pid, Explanation);
        false -> throw({error, {not_a_connection_pid, Pid}})
    end.

force_connection_event_refresh() ->
    [rabbit_reader:force_event_refresh(C) || C <- connections()],
    ok.

%%--------------------------------------------------------------------

tcp_host({0,0,0,0}) ->
    hostname();

tcp_host({0,0,0,0,0,0,0,0}) ->
    hostname();

tcp_host(IPAddress) ->
    case inet:gethostbyaddr(IPAddress) of
        {ok, #hostent{h_name = Name}} -> Name;
        {error, _Reason} -> rabbit_misc:ntoa(IPAddress)
    end.

hostname() ->
    {ok, Hostname} = inet:gethostname(),
    case inet:gethostbyname(Hostname) of
        {ok,    #hostent{h_name = Name}} -> Name;
        {error, _Reason}                 -> Hostname
    end.

cmap(F) -> rabbit_misc:filter_exit_map(F, connections()).

tcp_opts() ->
    {ok, Opts} = application:get_env(rabbit, tcp_listen_options),
    Opts.

%% inet_parse:address takes care of ip string, like "0.0.0.0"
%% inet:getaddr returns immediately for ip tuple {0,0,0,0},
%%  and runs 'inet_gethost' port process for dns lookups.
%% On Windows inet:getaddr runs dns resolver for ip string, which may fail.
getaddr(Host, Family) ->
    case inet_parse:address(Host) of
        {ok, IPAddress} -> [{IPAddress, resolve_family(IPAddress, Family)}];
        {error, _}      -> gethostaddr(Host, Family)
    end.

gethostaddr(Host, auto) ->
    Lookups = [{Family, inet:getaddr(Host, Family)} || Family <- [inet, inet6]],
    case [{IP, Family} || {Family, {ok, IP}} <- Lookups] of
        []  -> host_lookup_error(Host, Lookups);
        IPs -> IPs
    end;

gethostaddr(Host, Family) ->
    case inet:getaddr(Host, Family) of
        {ok, IPAddress} -> [{IPAddress, Family}];
        {error, Reason} -> host_lookup_error(Host, Reason)
    end.

host_lookup_error(Host, Reason) ->
    error_logger:error_msg("invalid host ~p - ~p~n", [Host, Reason]),
    throw({error, {invalid_host, Host, Reason}}).

resolve_family({_,_,_,_},         auto) -> inet;
resolve_family({_,_,_,_,_,_,_,_}, auto) -> inet6;
resolve_family(IP,                auto) -> throw({error, {strange_family, IP}});
resolve_family(_,                 F)    -> F.

%%--------------------------------------------------------------------

%% There are three kinds of machine (for our purposes).
%%
%% * Those which treat IPv4 addresses as a special kind of IPv6 address
%%   ("Single stack")
%%   - Linux by default, Windows Vista and later
%%   - We also treat any (hypothetical?) IPv6-only machine the same way
%% * Those which consider IPv6 and IPv4 to be completely separate things
%%   ("Dual stack")
%%   - OpenBSD, Windows XP / 2003, Linux if so configured
%% * Those which do not support IPv6.
%%   - Ancient/weird OSes, Linux if so configured
%%
%% How to reconfigure Linux to test this:
%% Single stack (default):
%% echo 0 > /proc/sys/net/ipv6/bindv6only
%% Dual stack:
%% echo 1 > /proc/sys/net/ipv6/bindv6only
%% IPv4 only:
%% add ipv6.disable=1 to GRUB_CMDLINE_LINUX_DEFAULT in /etc/default/grub then
%% sudo update-grub && sudo reboot
%%
%% This matters in (and only in) the case where the sysadmin (or the
%% app descriptor) has only supplied a port and we wish to bind to
%% "all addresses". This means different things depending on whether
%% we're single or dual stack. On single stack binding to "::"
%% implicitly includes all IPv4 addresses, and subsequently attempting
%% to bind to "0.0.0.0" will fail. On dual stack, binding to "::" will
%% only bind to IPv6 addresses, and we need another listener bound to
%% "0.0.0.0" for IPv4. Finally, on IPv4-only systems we of course only
%% want to bind to "0.0.0.0".
%%
%% Unfortunately it seems there is no way to detect single vs dual stack
%% apart from attempting to bind to the port.
port_to_listeners(Port) ->
    IPv4 = {"0.0.0.0", Port, inet},
    IPv6 = {"::",      Port, inet6},
    case ipv6_status(?FIRST_TEST_BIND_PORT) of
        single_stack -> [IPv6];
        ipv6_only    -> [IPv6];
        dual_stack   -> [IPv6, IPv4];
        ipv4_only    -> [IPv4]
    end.

ipv6_status(TestPort) ->
    IPv4 = [inet,  {ip, {0,0,0,0}}],
    IPv6 = [inet6, {ip, {0,0,0,0,0,0,0,0}}],
    case gen_tcp:listen(TestPort, IPv6) of
        {ok, LSock6} ->
            case gen_tcp:listen(TestPort, IPv4) of
                {ok, LSock4} ->
                    %% Dual stack
                    gen_tcp:close(LSock6),
                    gen_tcp:close(LSock4),
                    dual_stack;
                %% Checking the error here would only let us
                %% distinguish single stack IPv6 / IPv4 vs IPv6 only,
                %% which we figure out below anyway.
                {error, _} ->
                    gen_tcp:close(LSock6),
                    case gen_tcp:listen(TestPort, IPv4) of
                        %% Single stack
                        {ok, LSock4}            -> gen_tcp:close(LSock4),
                                                   single_stack;
                        %% IPv6-only machine. Welcome to the future.
                        {error, eafnosupport}   -> ipv6_only; %% Linux
                        {error, eprotonosupport}-> ipv6_only; %% FreeBSD
                        %% Dual stack machine with something already
                        %% on IPv4.
                        {error, _}              -> ipv6_status(TestPort + 1)
                    end
            end;
        %% IPv4-only machine. Welcome to the 90s.
        {error, eafnosupport} -> %% Linux
            ipv4_only;
        {error, eprotonosupport} -> %% FreeBSD
            ipv4_only;
        %% Port in use
        {error, _} ->
            ipv6_status(TestPort + 1)
    end.
