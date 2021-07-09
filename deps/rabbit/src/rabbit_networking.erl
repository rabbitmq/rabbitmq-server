%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_networking).

%% This module contains various functions that deal with networking,
%% TCP and TLS listeners, and connection information.
%%
%% It also contains a boot step — boot/0 — that starts networking machinery.
%% This module primarily covers AMQP 0-9-1 but some bits are reused in
%% plugins that provide protocol support, e.g. STOMP or MQTT.
%%
%% Functions in this module take care of normalising TCP listener options,
%% including dual IP stack cases, and starting the AMQP 0-9-1 listener(s).
%%
%% See also tcp_listener_sup and tcp_listener.

-export([boot/0, start_tcp_listener/2, start_tcp_listener/3,
         start_ssl_listener/3, start_ssl_listener/4,
         stop_tcp_listener/1, on_node_down/1, active_listeners/0,
         node_listeners/1, node_client_listeners/1,
         register_connection/1, unregister_connection/1,
         register_non_amqp_connection/1, unregister_non_amqp_connection/1,
         connections/0, non_amqp_connections/0, connection_info_keys/0,
         connection_info/1, connection_info/2,
         connection_info_all/0, connection_info_all/1,
         emit_connection_info_all/4, emit_connection_info_local/3,
         close_connection/2, close_connections/2, close_all_connections/1,
         close_all_user_connections/2,
         force_connection_event_refresh/1, force_non_amqp_connection_event_refresh/1,
         handshake/2, tcp_host/1,
         ranch_ref/1, ranch_ref/2, ranch_ref_of_protocol/1,
         listener_of_protocol/1, stop_ranch_listener_of_protocol/1]).

%% Used by TCP-based transports, e.g. STOMP adapter
-export([tcp_listener_addresses/1,
         tcp_listener_spec/9, tcp_listener_spec/10,
         ensure_ssl/0, fix_ssl_options/1, poodle_check/1]).

-export([tcp_listener_started/4, tcp_listener_stopped/4]).

-deprecated([{force_connection_event_refresh, 1, eventually}]).

-export([
    local_connections/0,
    local_non_amqp_connections/0,
    %% prefer local_connections/0
    connections_local/0
]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_misc.hrl").

%% IANA-suggested ephemeral port range is 49152 to 65535
-define(FIRST_TEST_BIND_PORT, 49152).

%%----------------------------------------------------------------------------

-export_type([ip_port/0, hostname/0]).

-type hostname() :: rabbit_net:hostname().
-type ip_port() :: rabbit_net:ip_port().

-type family() :: atom().
-type listener_config() :: ip_port() |
                           {hostname(), ip_port()} |
                           {hostname(), ip_port(), family()}.
-type address() :: {inet:ip_address(), ip_port(), family()}.
-type name_prefix() :: atom().
-type protocol() :: atom().
-type label() :: string().

-spec boot() -> 'ok' | no_return().

boot() ->
    ok = record_distribution_listener(),
    _ = application:start(ranch),
    rabbit_log:debug("Started Ranch"),
    %% Failures will throw exceptions
    _ = boot_listeners(fun boot_tcp/2, application:get_env(rabbit, num_tcp_acceptors, 10),
                       application:get_env(rabbit, num_conns_sups, 1), "TCP"),
    _ = boot_listeners(fun boot_tls/2, application:get_env(rabbit, num_ssl_acceptors, 10),
                       application:get_env(rabbit, num_conns_sups, 1), "TLS"),
    ok.

boot_listeners(Fun, NumAcceptors, ConcurrentConnsSupsCount, Type) ->
    case Fun(NumAcceptors, ConcurrentConnsSupsCount) of
        ok                                                                  ->
            ok;
        {error, {could_not_start_listener, Address, Port, Details}} = Error ->
            rabbit_log:error("Failed to start ~s listener [~s]:~p, error: ~p",
                             [Type, Address, Port, Details]),
            throw(Error)
    end.

boot_tcp(NumAcceptors, ConcurrentConnsSupsCount) ->
    {ok, TcpListeners} = application:get_env(tcp_listeners),
    case lists:foldl(fun(Listener, ok) ->
                             start_tcp_listener(Listener, NumAcceptors, ConcurrentConnsSupsCount);
                        (_Listener, Error) ->
                             Error
                     end,
                     ok, TcpListeners) of
        ok                 -> ok;
        {error, _} = Error -> Error
    end.

boot_tls(NumAcceptors, ConcurrentConnsSupsCount) ->
    case application:get_env(ssl_listeners) of
        {ok, []} ->
            ok;
        {ok, SslListeners} ->
            SslOpts = ensure_ssl(),
            case poodle_check('AMQP') of
                ok     -> [start_ssl_listener(L, SslOpts, NumAcceptors, ConcurrentConnsSupsCount)
                           || L <- SslListeners];
                danger -> ok
            end,
            ok
    end.

-spec ensure_ssl() -> rabbit_types:infos().

ensure_ssl() ->
    {ok, SslAppsConfig} = application:get_env(rabbit, ssl_apps),
    ok = app_utils:start_applications(SslAppsConfig),
    {ok, SslOptsConfig0} = application:get_env(rabbit, ssl_options),
    rabbit_ssl_options:fix(SslOptsConfig0).

-spec poodle_check(atom()) -> 'ok' | 'danger'.

poodle_check(Context) ->
    {ok, Vsn} = application:get_key(ssl, vsn),
    case rabbit_misc:version_compare(Vsn, "5.3", gte) of %% R16B01
        true  -> ok;
        false -> case application:get_env(rabbit, ssl_allow_poodle_attack) of
                     {ok, true}  -> ok;
                     _           -> log_poodle_fail(Context),
                                    danger
                 end
    end.

log_poodle_fail(Context) ->
    rabbit_log:error(
      "The installed version of Erlang (~s) contains the bug OTP-10905,~n"
      "which makes it impossible to disable SSLv3. This makes the system~n"
      "vulnerable to the POODLE attack. SSL listeners for ~s have therefore~n"
      "been disabled.~n~n"
      "You are advised to upgrade to a recent Erlang version; R16B01 is the~n"
      "first version in which this bug is fixed, but later is usually~n"
      "better.~n~n"
      "If you cannot upgrade now and want to re-enable SSL listeners, you can~n"
      "set the config item 'ssl_allow_poodle_attack' to 'true' in the~n"
      "'rabbit' section of your configuration file.",
      [rabbit_misc:otp_release(), Context]).

fix_ssl_options(Config) ->
    rabbit_ssl_options:fix(Config).

-spec tcp_listener_addresses(listener_config()) -> [address()].

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
    rabbit_log:error("invalid port ~p - not 0..65535", [Port]),
    throw({error, {invalid_port, Port}}).

tcp_listener_addresses_auto(Port) ->
    lists:append([tcp_listener_addresses(Listener) ||
                     Listener <- port_to_listeners(Port)]).

tcp_listener_spec(NamePrefix, Address, SocketOpts, Transport, ProtoSup, ProtoOpts,
                  Protocol, NumAcceptors, Label) ->
    tcp_listener_spec(NamePrefix, Address, SocketOpts, Transport, ProtoSup, ProtoOpts,
                      Protocol, NumAcceptors, 1, Label).

-spec tcp_listener_spec
        (name_prefix(), address(), [gen_tcp:listen_option()], module(), module(),
         any(), protocol(), non_neg_integer(), non_neg_integer(), label()) ->
            supervisor:child_spec().

tcp_listener_spec(NamePrefix, {IPAddress, Port, Family}, SocketOpts,
                  Transport, ProtoSup, ProtoOpts, Protocol, NumAcceptors,
                  ConcurrentConnsSupsCount, Label) ->
    Args = [IPAddress, Port, Transport, [Family | SocketOpts], ProtoSup, ProtoOpts,
            {?MODULE, tcp_listener_started, [Protocol, SocketOpts]},
            {?MODULE, tcp_listener_stopped, [Protocol, SocketOpts]},
            NumAcceptors, ConcurrentConnsSupsCount, Label],
    {rabbit_misc:tcp_name(NamePrefix, IPAddress, Port),
     {tcp_listener_sup, start_link, Args},
     transient, infinity, supervisor, [tcp_listener_sup]}.

-spec ranch_ref(#listener{} | [{atom(), any()}] | 'undefined') -> ranch:ref() | undefined.
ranch_ref(#listener{port = Port}) ->
    [{IPAddress, Port, _Family} | _] = tcp_listener_addresses(Port),
    {acceptor, IPAddress, Port};
ranch_ref(Listener) when is_list(Listener) ->
    Port = rabbit_misc:pget(port, Listener),
    [{IPAddress, Port, _Family} | _] = tcp_listener_addresses(Port),
    {acceptor, IPAddress, Port};
ranch_ref(undefined) ->
    undefined.

-spec ranch_ref(inet:ip_address(), ip_port()) -> ranch:ref().

%% Returns a reference that identifies a TCP listener in Ranch.
ranch_ref(IPAddress, Port) ->
    {acceptor, IPAddress, Port}.

-spec ranch_ref_of_protocol(atom()) -> ranch:ref() | undefined.
ranch_ref_of_protocol(Protocol) ->
    ranch_ref(listener_of_protocol(Protocol)).

-spec listener_of_protocol(atom()) -> #listener{}.
listener_of_protocol(Protocol) ->
    rabbit_misc:execute_mnesia_transaction(
        fun() ->
            MatchSpec = #listener{
                node = node(),
                protocol = Protocol,
                _ = '_'
            },
            case mnesia:match_object(rabbit_listener, MatchSpec, read) of
                []    -> undefined;
                [Row] -> Row
            end
        end).

-spec stop_ranch_listener_of_protocol(atom()) -> ok | {error, not_found}.
stop_ranch_listener_of_protocol(Protocol) ->
    case rabbit_networking:ranch_ref_of_protocol(Protocol) of
        undefined -> ok;
        Ref       ->
            rabbit_log:debug("Stopping Ranch listener for protocol ~s", [Protocol]),
            ranch:stop_listener(Ref)
    end.

-spec start_tcp_listener(
        listener_config(), integer()) -> 'ok' | {'error', term()}.

start_tcp_listener(Listener, NumAcceptors) ->
    start_tcp_listener(Listener, NumAcceptors, 1).

-spec start_tcp_listener(
        listener_config(), integer(), integer()) -> 'ok' | {'error', term()}.

start_tcp_listener(Listener, NumAcceptors, ConcurrentConnsSupsCount) ->
    start_listener(Listener, NumAcceptors, ConcurrentConnsSupsCount, amqp,
                   "TCP listener", tcp_opts()).

-spec start_ssl_listener(
        listener_config(), rabbit_types:infos(), integer()) -> 'ok' | {'error', term()}.

start_ssl_listener(Listener, SslOpts, NumAcceptors) ->
    start_ssl_listener(Listener, SslOpts, NumAcceptors, 1).

-spec start_ssl_listener(
        listener_config(), rabbit_types:infos(), integer(), integer()) -> 'ok' | {'error', term()}.

start_ssl_listener(Listener, SslOpts, NumAcceptors, ConcurrentConnsSupsCount) ->
    start_listener(Listener, NumAcceptors, ConcurrentConnsSupsCount, 'amqp/ssl',
                   "TLS (SSL) listener", tcp_opts() ++ SslOpts).

-spec start_listener(
        listener_config(), integer(), integer(), protocol(), label(), list()) ->
          'ok' | {'error', term()}.
start_listener(Listener, NumAcceptors, ConcurrentConnsSupsCount, Protocol, Label, Opts) ->
    lists:foldl(fun (Address, ok) ->
                        start_listener0(Address, NumAcceptors, ConcurrentConnsSupsCount, Protocol,
                                        Label, Opts);
                    (_Address, {error, _} = Error) ->
                        Error
                end, ok, tcp_listener_addresses(Listener)).

start_listener0(Address, NumAcceptors, ConcurrentConnsSupsCount, Protocol, Label, Opts) ->
    Transport = transport(Protocol),
    Spec = tcp_listener_spec(rabbit_tcp_listener_sup, Address, Opts,
                             Transport, rabbit_connection_sup, [], Protocol,
                             NumAcceptors, ConcurrentConnsSupsCount, Label),
    case supervisor:start_child(rabbit_sup, Spec) of
        {ok, _}          -> ok;
        {error, {{shutdown, {failed_to_start_child, _,
                             {shutdown, {failed_to_start_child, _,
                                         {listen_error, _, PosixError}}}}}, _}} ->
            {IPAddress, Port, _Family} = Address,
            {error, {could_not_start_listener, rabbit_misc:ntoa(IPAddress), Port, PosixError}};
        {error, Other} ->
            {IPAddress, Port, _Family} = Address,
            {error, {could_not_start_listener, rabbit_misc:ntoa(IPAddress), Port, Other}}
    end.

transport(Protocol) ->
    case Protocol of
        amqp       -> ranch_tcp;
        'amqp/ssl' -> ranch_ssl
    end.

-spec stop_tcp_listener(listener_config()) -> 'ok'.

stop_tcp_listener(Listener) ->
    [stop_tcp_listener0(Address) ||
        Address <- tcp_listener_addresses(Listener)],
    ok.

stop_tcp_listener0({IPAddress, Port, _Family}) ->
    Name = rabbit_misc:tcp_name(rabbit_tcp_listener_sup, IPAddress, Port),
    ok = supervisor:terminate_child(rabbit_sup, Name),
    ok = supervisor:delete_child(rabbit_sup, Name).

-spec tcp_listener_started
        (_, _,
         string() |
         {byte(),byte(),byte(),byte()} |
         {char(),char(),char(),char(),char(),char(),char(),char()}, _) ->
            'ok'.

tcp_listener_started(Protocol, Opts, IPAddress, Port) ->
    %% We need the ip to distinguish e.g. 0.0.0.0 and 127.0.0.1
    %% We need the host so we can distinguish multiple instances of the above
    %% in a cluster.
    ok = mnesia:dirty_write(
           rabbit_listener,
           #listener{node = node(),
                     protocol = Protocol,
                     host = tcp_host(IPAddress),
                     ip_address = IPAddress,
                     port = Port,
                     opts = Opts}).

-spec tcp_listener_stopped
        (_, _,
         string() |
         {byte(),byte(),byte(),byte()} |
         {char(),char(),char(),char(),char(),char(),char(),char()},
         _) ->
            'ok'.

tcp_listener_stopped(Protocol, Opts, IPAddress, Port) ->
    ok = mnesia:dirty_delete_object(
           rabbit_listener,
           #listener{node = node(),
                     protocol = Protocol,
                     host = tcp_host(IPAddress),
                     ip_address = IPAddress,
                     port = Port,
                     opts = Opts}).

-spec record_distribution_listener() -> ok | no_return().

record_distribution_listener() ->
    {Name, Host} = rabbit_nodes:parts(node()),
    case erl_epmd:port_please(list_to_atom(Name), Host, infinity) of
        {port, Port, _Version} ->
            tcp_listener_started(clustering, [], {0,0,0,0,0,0,0,0}, Port);
        noport ->
            throw({error, no_epmd_port})
    end.

-spec active_listeners() -> [rabbit_types:listener()].

active_listeners() ->
    rabbit_misc:dirty_read_all(rabbit_listener).

-spec node_listeners(node()) -> [rabbit_types:listener()].

node_listeners(Node) ->
    mnesia:dirty_read(rabbit_listener, Node).

-spec node_client_listeners(node()) -> [rabbit_types:listener()].

node_client_listeners(Node) ->
    case node_listeners(Node) of
        [] -> [];
        Xs ->
            lists:filter(fun (#listener{protocol = clustering}) -> false;
                             (_) -> true
                         end, Xs)
    end.

-spec on_node_down(node()) -> 'ok'.

on_node_down(Node) ->
    case lists:member(Node, nodes()) of
        false ->
            rabbit_log:info(
                   "Node ~s is down, deleting its listeners", [Node]),
            ok = mnesia:dirty_delete(rabbit_listener, Node);
        true  ->
            rabbit_log:info(
                   "Keeping ~s listeners: the node is already back", [Node])
    end.

-spec register_connection(pid()) -> ok.

register_connection(Pid) -> pg_local:join(rabbit_connections, Pid).

-spec unregister_connection(pid()) -> ok.

unregister_connection(Pid) -> pg_local:leave(rabbit_connections, Pid).

-spec connections() -> [rabbit_types:connection()].

connections() ->
    Nodes = rabbit_nodes:all_running(),
    rabbit_misc:append_rpc_all_nodes(Nodes, rabbit_networking, connections_local, [], ?RPC_TIMEOUT).

-spec local_connections() -> [rabbit_types:connection()].
%% @doc Returns pids of AMQP 0-9-1 and AMQP 1.0 connections local to this node.
local_connections() ->
    connections_local().

-spec connections_local() -> [rabbit_types:connection()].
%% @deprecated Prefer {@link local_connections}
connections_local() -> pg_local:get_members(rabbit_connections).

-spec register_non_amqp_connection(pid()) -> ok.

register_non_amqp_connection(Pid) -> pg_local:join(rabbit_non_amqp_connections, Pid).

-spec unregister_non_amqp_connection(pid()) -> ok.

unregister_non_amqp_connection(Pid) -> pg_local:leave(rabbit_non_amqp_connections, Pid).

-spec non_amqp_connections() -> [rabbit_types:connection()].

non_amqp_connections() ->
  Nodes = rabbit_nodes:all_running(),
  rabbit_misc:append_rpc_all_nodes(Nodes, rabbit_networking, local_non_amqp_connections, [], ?RPC_TIMEOUT).

-spec local_non_amqp_connections() -> [rabbit_types:connection()].
local_non_amqp_connections() ->
  pg_local:get_members(rabbit_non_amqp_connections).

-spec connection_info_keys() -> rabbit_types:info_keys().

connection_info_keys() -> rabbit_reader:info_keys().

-spec connection_info(rabbit_types:connection()) -> rabbit_types:infos().

connection_info(Pid) -> rabbit_reader:info(Pid).

-spec connection_info(rabbit_types:connection(), rabbit_types:info_keys()) ->
          rabbit_types:infos().

connection_info(Pid, Items) -> rabbit_reader:info(Pid, Items).

-spec connection_info_all() -> [rabbit_types:infos()].

connection_info_all() -> cmap(fun (Q) -> connection_info(Q) end).

-spec connection_info_all(rabbit_types:info_keys()) ->
          [rabbit_types:infos()].

connection_info_all(Items) -> cmap(fun (Q) -> connection_info(Q, Items) end).

emit_connection_info_all(Nodes, Items, Ref, AggregatorPid) ->
    Pids = [ spawn_link(Node, rabbit_networking, emit_connection_info_local, [Items, Ref, AggregatorPid]) || Node <- Nodes ],
    rabbit_control_misc:await_emitters_termination(Pids),
    ok.

emit_connection_info_local(Items, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map_with_exit_handler(
      AggregatorPid, Ref, fun(Q) -> connection_info(Q, Items) end,
      connections_local()).

-spec close_connection(pid(), string()) -> 'ok'.

close_connection(Pid, Explanation) ->
    case lists:member(Pid, connections()) of
        true  ->
            Res = rabbit_reader:shutdown(Pid, Explanation),
            rabbit_log:info("Closing connection ~p because ~p", [Pid, Explanation]),
            Res;
        false ->
            rabbit_log:warning("Asked to close connection ~p (reason: ~p) "
                               "but no running cluster node reported it as an active connection. Was it already closed? ",
                               [Pid, Explanation]),
            ok
    end.

-spec close_connections([pid()], string()) -> 'ok'.
close_connections(Pids, Explanation) ->
    [close_connection(Pid, Explanation) || Pid <- Pids],
    ok.

-spec close_all_user_connections(rabbit_types:username(), string()) -> 'ok'.
close_all_user_connections(Username, Explanation) ->
    Pids = [Pid || #tracked_connection{pid = Pid} <- rabbit_connection_tracking:list_of_user(Username)],
    [close_connection(Pid, Explanation) || Pid <- Pids],
    ok.

%% Meant to be used by tests only
-spec close_all_connections(string()) -> 'ok'.
close_all_connections(Explanation) ->
    Pids = connections(),
    [close_connection(Pid, Explanation) || Pid <- Pids],
    ok.

-spec force_connection_event_refresh(reference()) -> 'ok'.
force_connection_event_refresh(Ref) ->
    [rabbit_reader:force_event_refresh(C, Ref) || C <- connections()],
    ok.

-spec force_non_amqp_connection_event_refresh(reference()) -> 'ok'.
force_non_amqp_connection_event_refresh(Ref) ->
  [gen_server:cast(Pid, {force_event_refresh, Ref}) || Pid <- non_amqp_connections()],
  ok.

-spec failed_to_recv_proxy_header(_, _) -> no_return().
failed_to_recv_proxy_header(Ref, Error) ->
    Msg = case Error of
        closed -> "error when receiving proxy header: TCP socket was ~p prematurely";
        _Other -> "error when receiving proxy header: ~p"
    end,
    rabbit_log:debug(Msg, [Error]),
    % The following call will clean up resources then exit
    _ = ranch:handshake(Ref),
    exit({shutdown, failed_to_recv_proxy_header}).

handshake(Ref, ProxyProtocolEnabled) ->
    case ProxyProtocolEnabled of
        true ->
            case ranch:recv_proxy_header(Ref, 3000) of
                {error, Error} ->
                    failed_to_recv_proxy_header(Ref, Error);
                {error, protocol_error, Error} ->
                    failed_to_recv_proxy_header(Ref, Error);
                {ok, ProxyInfo} ->
                    {ok, Sock} = ranch:handshake(Ref),
                    setup_socket(Sock),
                    {ok, {rabbit_proxy_socket, Sock, ProxyInfo}}
            end;
        false ->
            {ok, Sock} = ranch:handshake(Ref),
            setup_socket(Sock),
            {ok, Sock}
    end.

setup_socket(Sock) ->
    ok = tune_buffer_size(Sock),
    ok = file_handle_cache:obtain().

tune_buffer_size(Sock) ->
    case tune_buffer_size1(Sock) of
        ok         -> ok;
        {error, _} -> rabbit_net:fast_close(Sock),
                      exit(normal)
    end.

tune_buffer_size1(Sock) ->
    case rabbit_net:getopts(Sock, [sndbuf, recbuf, buffer]) of
        {ok, BufSizes} -> BufSz = lists:max([Sz || {_Opt, Sz} <- BufSizes]),
                          rabbit_net:setopts(Sock, [{buffer, BufSz}]);
        Error          -> Error
    end.

%%--------------------------------------------------------------------

tcp_host(IPAddress) ->
    rabbit_net:tcp_host(IPAddress).

cmap(F) -> rabbit_misc:filter_exit_map(F, connections()).

tcp_opts() ->
    {ok, ConfigOpts} = application:get_env(rabbit, tcp_listen_options),
    ConfigOpts.

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

-spec host_lookup_error(_, _) -> no_return().
host_lookup_error(Host, Reason) ->
    rabbit_log:error("invalid host ~p - ~p", [Host, Reason]),
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
