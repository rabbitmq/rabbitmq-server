%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% @todo After all is done I would like to rename
%%       rabbit_web_dispatch to simply rabbit_web.
%%       That application will provide the core functionality
%%       of all RabbitMQ Web applications.

%% @todo It would be great to have a shared rabbit_web
%%       listener. Since all Web applications depend on
%%       rabbit_web, it would start first, and the
%%       other applications would just update it with
%%       additional routes and Websocket endpoints.
%%       In these applications they would just specify
%%       listener information as "rabbit_web".

-module(rabbit_web).

-export([start_listeners/3]).
-export([get_websocket_config_for_ref/1]).
-export([ws_route/1]).

%% We create listeners in sets. From the configuration
%% we obtain a BaseRef (identifying the set) and a
%% number of different transports and transport options.
%% In addition we deduct the transport family by doing
%% a lookup. We may as a result create an IPv4 listener,
%% an IPv6 listener, or both in dual stack setups. We
%% go one step further: we group the TCP, TLS and QUIC
%% listeners to have a single start function for all
%% of them. This is because we want to automatically
%% populate Alt-Svc with those alternate services, so
%% that clients can choose the most appropriate
%% listener for their needs. This is also required
%% for browsers to discover QUIC endpoints- the only
%% alternative being the HTTPS DNS record.
%%
%% In the future we may want to add support for local
%% sockets. The name scheme allows for that.
%%
%% Should this model be applied to all listeners- and not
%% just Web listeners- then the following listeners could
%% be created:
%%
%% BaseRef   :: amqp_legacy | amqp | mqtt | stomp | stream
%%            | web_mqtt | web_stomp | management
%% Transport :: tcp | tls | quic
%% Num       :: pos_integer()
%% Family    :: inet | inet6 | local
%% Ref       :: {BaseRef, Transport, Num, Family}
%%
%% There could be additional existing or new listeners
%% such as web_amqp, web_stream and more.
%%
%% The Num variable is there to allow configuring multiple
%% listeners for the same protocol. For example one could
%% have a listeners.tcp.1, listeners.tcp.2 and listeners.tcp.3
%% in their configuration file.
%%
%% There is no need to distinguish between different
%% protocols that use the same transport; Cowboy doesn't
%% either. So if 'tls' is used then the protocol options
%% will be shared between HTTP/1.1 and HTTP/2 and the
%% appropriate protocol selected using ALPN. On the
%% other hand when transports differ so may protocol
%% configuration. One could imagine HTTP force-redirecting
%% to HTTPS as one example of use case.
%%
%% We also provide the ability to share the Websocket endpoint
%% between different plugins. Because Websocket is built to
%% handle multiple sub-protocols, and plugins typically
%% implement their own set, we can use a single Websocket
%% endpoint for all of them, if configured to do so.
%%
%% We provide information about the Websocket endpoint in
%% the protocol options and keep it around in a persistent
%% term.

-type websocket_config() :: #{
    %% @todo Have an option that allows selecting one without a header? For STOMP.
    %%       Perhaps only accept a default if there is only 1 handler defined for a ref.
    protocols := [binary()],
    handler := module(),
    opts := any()
}.

-type config() :: #{
    %% The configuration must include at least one listener.
    %%
    %% The listener number is the position in the list. Take extra
    %% care to match with the configuration entry (tcp.listener.1)
    %% for easier debugging.
    tcp | tls | quic := [rabbit_networking:listening_config()],
    tcp_opts => todo,
    tls_opts => todo,
    quic_opts => todo,
    %% For the time being we only support a single Websocket endpoint
    %% per listener, and we expect that endpoint to be "/ws". If it later
    %% becomes necessary to support more (why though?), we can either
    %% extend the map or wrap the map in a list of endpoints.
    websocket => websocket_config()
}.

%% The type of the produced listener ref.

-type ref() :: {any(), tcp | tls | quic, pos_integer(), inet | inet6}.

%% --

-spec start_listeners(atom(), config(), map())
    -> {ok, [ref()]}.

start_listeners(BaseRef, Transports, ProtoOpts0) ->
    %% Add common rabbit_web components to protocol options.
    ProtoOpts = prepare_proto_opts(ProtoOpts0),
    %% In order to populate Alt-Svc and other alternative
    %% services mechanisms, we must find whether these
    %% alternative services are defined in the transports.
    %% In general we want to advertise later versions of
    %% HTTP in previous versions: for example HTTP/1.1
    %% advertises HTTP/2 and HTTP/3; while HTTP/2 can
    %% advertise HTTP/3 only. So we start QUIC first.
    {QuicRefs, QuicAltSvcs} = start_quic(BaseRef, Transports, ProtoOpts),
    %% Then we start TLS as it may include HTTP/2.
    {TLSRefs, TLSAltSvcs} = start_tls(BaseRef, Transports, ProtoOpts,
        QuicAltSvcs),
    %% Finally TCP.
    TCPRefs = start_tcp(BaseRef, Transports, ProtoOpts,
        QuicAltSvcs ++ TLSAltSvcs),
    %% Next we want to keep the Websocket protocols supported.
    store_websocket_info(BaseRef, Transports),
    %% We return all refs.
    {ok, TCPRefs ++ TLSRefs ++ QuicRefs}.

prepare_proto_opts(Opts) ->
    Opts#{
        %% @todo Middlewares?
        %% @todo Access logs etc. Use metrics_h?
        stream_handlers => [cowboy_stream_h],
        enable_connect_protocol => true
    }.

%% @todo Implement QUIC.
start_quic(_, _, _) ->
    {[], []}.

%% @todo Implement TLS.
start_tls(_, _, _, _) ->
    {[], []}.

start_tcp(BaseRef, #{tcp := Listeners, tcp_opts := TransOpts}, ProtoOpts, []) ->
    %% No alternative services.
    start_tcp(BaseRef, Listeners, 1, TransOpts, ProtoOpts, []);
start_tcp(BaseRef, #{tcp := Listeners, tcp_opts := TransOpts}, ProtoOpts, AltSvcs) ->
    start_tcp(BaseRef, Listeners, 1, TransOpts, ProtoOpts#{
        %% The Alt-Svc header will be set by our stream handler.
        alternative_services => AltSvcs
    }, []);
%% There were no TCP listeners.
start_tcp(_, _, _, _) ->
    [].

start_tcp(_, [], _, _, _, Acc) ->
    lists:reverse(Acc);
start_tcp(BaseRef, [Listener|Tail], Num, TransOpts, ProtoOpts, Acc0) ->
    Addresses = rabbit_networking:tcp_listener_addresses(Listener),
    Acc = start_tcp_addrs(BaseRef, Addresses, Num, TransOpts, ProtoOpts, Acc0),
    start_tcp(BaseRef, Tail, Num + 1, TransOpts, ProtoOpts, Acc).

start_tcp_addrs(_, [], _, _, _, Acc) ->
    Acc;
start_tcp_addrs(BaseRef, [Addr|Tail], Num, TransOpts, ProtoOpts, Acc) ->
    {_Host, _Port, Family} = Addr,
    Ref = {BaseRef, tcp, Num, Family},
    {ok, _} = cowboy:start_clear(Ref, TransOpts, ProtoOpts),
    start_tcp_addrs(BaseRef, Tail, Num, TransOpts, ProtoOpts, [Ref|Acc]).

store_websocket_info(BaseRef, #{websocket := WsConfig}) ->
    %% @todo This should be done in a single process to avoid concurrency issues.
    Terms = persistent_term:get(rabbit_websocket, []),
    persistent_term:put(rabbit_websocket, [{BaseRef, WsConfig}|Terms]);
store_websocket_info(_, _) ->
    ok.

-spec get_websocket_config_for_ref(ref()) -> [websocket_config()].

get_websocket_config_for_ref({ThisBaseRef, _, _, _}) ->
    Terms = persistent_term:get(rabbit_websocket, []),
    [WsConfig || {BaseRef, WsConfig} <- Terms, BaseRef =:= ThisBaseRef].

%% It is occasionally necessary to update the list of routes
%% for Web listeners. For example a use case could be for
%% the Web-MQTT example application to add its routes to
%% the existing Web-MQTT listener.
%%
%% Because listeners can be configured in sets (due to
%% starting two listeners when using a dual IPv4/v6 stack),
%% and because the same listener isn't expected to serve
%% different routes on the same set of listeners, this
%% function takes only a BaseRef and applies the changes
%% to all the listeners it finds.

%update_listener_routes(BaseRef, fun(Dispatch) -> Dispatch end)
%    todo.

%% We use a common module to initiate all Websocket connections.
%% This module will select the appropriate Websocket handler
%% based on the requested sub-protocol.

-spec ws_route(Path) -> {Path, rabbit_web_ws_h, []}.

ws_route(Path) ->
    {Path, rabbit_web_ws_h, []}.
