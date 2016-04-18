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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
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

-export([boot/0, start_tcp_listener/2, start_ssl_listener/3,
         stop_tcp_listener/1, on_node_down/1, active_listeners/0,
         node_listeners/1, register_connection/1, unregister_connection/1,
         connections/0, connection_info_keys/0,
         connection_info/1, connection_info/2,
         connection_info_all/0, connection_info_all/1, connection_info_all/3,
         close_connection/2, force_connection_event_refresh/1, tcp_host/1]).

%% Used by TCP-based transports, e.g. STOMP adapter
-export([tcp_listener_addresses/1, tcp_listener_spec/9,
         ensure_ssl/0, fix_ssl_options/1, poodle_check/1]).

-export([tcp_listener_started/3, tcp_listener_stopped/3]).

%% Internal
-export([connections_local/0]).

-import(rabbit_misc, [pget/2, pget/3, pset/3]).

-include("rabbit.hrl").
-include_lib("kernel/include/inet.hrl").

%% IANA-suggested ephemeral port range is 49152 to 65535
-define(FIRST_TEST_BIND_PORT, 49152).

%% POODLE
-define(BAD_SSL_PROTOCOL_VERSIONS, [sslv3]).

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

-spec(start_tcp_listener/2 :: (listener_config(), integer()) -> 'ok').
-spec(start_ssl_listener/3 ::
        (listener_config(), rabbit_types:infos(), integer()) -> 'ok').
-spec(stop_tcp_listener/1 :: (listener_config()) -> 'ok').
-spec(active_listeners/0 :: () -> [rabbit_types:listener()]).
-spec(node_listeners/1 :: (node()) -> [rabbit_types:listener()]).
-spec(register_connection/1 :: (pid()) -> ok).
-spec(unregister_connection/1 :: (pid()) -> ok).
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
-spec(connection_info_all/3 ::
        (rabbit_types:info_keys(), reference(), pid()) -> 'ok').
-spec(close_connection/2 :: (pid(), string()) -> 'ok').
-spec(force_connection_event_refresh/1 :: (reference()) -> 'ok').

-spec(on_node_down/1 :: (node()) -> 'ok').
-spec(tcp_listener_addresses/1 :: (listener_config()) -> [address()]).
-spec(tcp_listener_spec/9 ::
        (name_prefix(), address(), [gen_tcp:listen_option()], module(), module(), protocol(), any(),
         non_neg_integer(), label()) -> supervisor:child_spec()).
-spec(ensure_ssl/0 :: () -> rabbit_types:infos()).
-spec(fix_ssl_options/1 :: (rabbit_types:infos()) -> rabbit_types:infos()).
-spec(poodle_check/1 :: (atom()) -> 'ok' | 'danger').

-spec(boot/0 :: () -> 'ok').
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
    ok = record_distribution_listener(),
    _ = application:start(ranch),
    ok = boot_tcp(application:get_env(rabbit, num_tcp_acceptors, 10)),
    ok = boot_ssl(application:get_env(rabbit, num_ssl_acceptors, 1)).

boot_tcp(NumAcceptors) ->
    {ok, TcpListeners} = application:get_env(tcp_listeners),
    [ok = start_tcp_listener(Listener, NumAcceptors) || Listener <- TcpListeners],
    ok.

boot_ssl(NumAcceptors) ->
    case application:get_env(ssl_listeners) of
        {ok, []} ->
            ok;
        {ok, SslListeners} ->
            SslOpts = ensure_ssl(),
            case poodle_check('AMQP') of
                ok     -> [start_ssl_listener(L, SslOpts, NumAcceptors) || L <- SslListeners];
                danger -> ok
            end,
            ok
    end.

ensure_ssl() ->
    {ok, SslAppsConfig} = application:get_env(rabbit, ssl_apps),
    ok = app_utils:start_applications(SslAppsConfig),
    {ok, SslOptsConfig} = application:get_env(rabbit, ssl_options),
    fix_ssl_options(SslOptsConfig).

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
      "'rabbit' section of your configuration file.~n",
      [rabbit_misc:otp_release(), Context]).

fix_ssl_options(Config) ->
    fix_verify_fun(fix_ssl_protocol_versions(Config)).

fix_verify_fun(SslOptsConfig) ->
    %% Starting with ssl 4.0.1 in Erlang R14B, the verify_fun function
    %% takes 3 arguments and returns a tuple.
    case rabbit_misc:pget(verify_fun, SslOptsConfig) of
        {Module, Function, InitialUserState} ->
            Fun = make_verify_fun(Module, Function, InitialUserState),
            rabbit_misc:pset(verify_fun, Fun, SslOptsConfig);
        {Module, Function} when is_atom(Module) ->
            Fun = make_verify_fun(Module, Function, none),
            rabbit_misc:pset(verify_fun, Fun, SslOptsConfig);
        {Verifyfun, _InitialUserState} when is_function(Verifyfun, 3) ->
            SslOptsConfig;
        undefined ->
            SslOptsConfig
    end.

make_verify_fun(Module, Function, InitialUserState) ->
    try
        %% Preload the module: it is required to use
        %% erlang:function_exported/3.
        Module:module_info()
    catch
        _:Exception ->
            rabbit_log:error("SSL verify_fun: module ~s missing: ~p~n",
                             [Module, Exception]),
            throw({error, {invalid_verify_fun, missing_module}})
    end,
    NewForm = erlang:function_exported(Module, Function, 3),
    OldForm = erlang:function_exported(Module, Function, 1),
    case {NewForm, OldForm} of
        {true, _} ->
            %% This verify_fun is supported by Erlang R14B+ (ssl
            %% 4.0.1 and later).
            Fun = fun(OtpCert, Event, UserState) ->
                    Module:Function(OtpCert, Event, UserState)
            end,
            {Fun, InitialUserState};
        {_, true} ->
            %% This verify_fun is supported by Erlang R14B+ for 
            %% undocumented backward compatibility.
            %%
            %% InitialUserState is ignored in this case.
            fun(Args) ->
                    Module:Function(Args)
            end;
        _ ->
            rabbit_log:error("SSL verify_fun: no ~s:~s/3 exported~n",
              [Module, Function]),
            throw({error, {invalid_verify_fun, function_not_exported}})
    end.

fix_ssl_protocol_versions(Config) ->
    case application:get_env(rabbit, ssl_allow_poodle_attack) of
        {ok, true} ->
            Config;
        _ ->
            Configured = case pget(versions, Config) of
                             undefined -> pget(available, ssl:versions(), []);
                             Vs        -> Vs
                         end,
            pset(versions, Configured -- ?BAD_SSL_PROTOCOL_VERSIONS, Config)
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
    rabbit_log:error("invalid port ~p - not 0..65535~n", [Port]),
    throw({error, {invalid_port, Port}}).

tcp_listener_addresses_auto(Port) ->
    lists:append([tcp_listener_addresses(Listener) ||
                     Listener <- port_to_listeners(Port)]).

tcp_listener_spec(NamePrefix, {IPAddress, Port, Family}, SocketOpts,
                  Transport, ProtoSup, ProtoOpts, Protocol, NumAcceptors, Label) ->
    {rabbit_misc:tcp_name(NamePrefix, IPAddress, Port),
     {tcp_listener_sup, start_link,
      [IPAddress, Port, Transport, [Family | SocketOpts], ProtoSup, ProtoOpts,
       {?MODULE, tcp_listener_started, [Protocol]},
       {?MODULE, tcp_listener_stopped, [Protocol]},
       NumAcceptors, Label]},
     transient, infinity, supervisor, [tcp_listener_sup]}.

start_tcp_listener(Listener, NumAcceptors) ->
    start_listener(Listener, NumAcceptors, amqp, "TCP Listener", tcp_opts()).

start_ssl_listener(Listener, SslOpts, NumAcceptors) ->
    start_listener(Listener, NumAcceptors, 'amqp/ssl', "SSL Listener", tcp_opts() ++ SslOpts).

start_listener(Listener, NumAcceptors, Protocol, Label, Opts) ->
    [start_listener0(Address, NumAcceptors, Protocol, Label, Opts) ||
        Address <- tcp_listener_addresses(Listener)],
    ok.

start_listener0(Address, NumAcceptors, Protocol, Label, Opts) ->
    Transport = case Protocol of
        amqp -> ranch_tcp;
        'amqp/ssl' -> ranch_ssl
    end,
    Spec = tcp_listener_spec(rabbit_tcp_listener_sup, Address, Opts,
                             Transport, rabbit_connection_sup, [], Protocol,
                             NumAcceptors, Label),
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

record_distribution_listener() ->
    {Name, Host} = rabbit_nodes:parts(node()),
    {port, Port, _Version} = erl_epmd:port_please(Name, Host),
    tcp_listener_started(clustering, {0,0,0,0,0,0,0,0}, Port).

active_listeners() ->
    rabbit_misc:dirty_read_all(rabbit_listener).

node_listeners(Node) ->
    mnesia:dirty_read(rabbit_listener, Node).

on_node_down(Node) ->
    case lists:member(Node, nodes()) of
        false -> ok = mnesia:dirty_delete(rabbit_listener, Node);
        true  -> rabbit_log:info(
                   "Keep ~s listeners: the node is already back~n", [Node])
    end.

register_connection(Pid) -> pg_local:join(rabbit_connections, Pid).

unregister_connection(Pid) -> pg_local:leave(rabbit_connections, Pid).

connections() ->
    rabbit_misc:append_rpc_all_nodes(rabbit_mnesia:cluster_nodes(running),
                                     rabbit_networking, connections_local, []).

connections_local() -> pg_local:get_members(rabbit_connections).

connection_info_keys() -> rabbit_reader:info_keys().

connection_info(Pid) -> rabbit_reader:info(Pid).
connection_info(Pid, Items) -> rabbit_reader:info(Pid, Items).

connection_info_all() -> cmap(fun (Q) -> connection_info(Q) end).
connection_info_all(Items) -> cmap(fun (Q) -> connection_info(Q, Items) end).

connection_info_all(Items, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map_with_exit_handler(
      AggregatorPid, Ref, fun(Q) -> connection_info(Q, Items) end,
      connections()).

close_connection(Pid, Explanation) ->
    rabbit_log:info("Closing connection ~p because ~p~n", [Pid, Explanation]),
    case lists:member(Pid, connections()) of
        true  -> rabbit_reader:shutdown(Pid, Explanation);
        false -> throw({error, {not_a_connection_pid, Pid}})
    end.

force_connection_event_refresh(Ref) ->
    [rabbit_reader:force_event_refresh(C, Ref) || C <- connections()],
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

host_lookup_error(Host, Reason) ->
    rabbit_log:error("invalid host ~p - ~p~n", [Host, Reason]),
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
