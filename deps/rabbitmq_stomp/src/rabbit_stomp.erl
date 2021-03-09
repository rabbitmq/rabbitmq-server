%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stomp).

-include("rabbit_stomp.hrl").

-behaviour(application).
-export([start/2, stop/1]).
-export([parse_default_user/2]).
-export([connection_info_local/1,
         emit_connection_info_local/3,
         emit_connection_info_all/4,
         list/0,
         close_all_client_connections/1]).

-define(DEFAULT_CONFIGURATION,
        #stomp_configuration{
          default_login    = undefined,
          default_passcode = undefined,
          implicit_connect = false,
          ssl_cert_login   = false}).

start(normal, []) ->
    Config = parse_configuration(),
    Listeners = parse_listener_configuration(),
    Result = rabbit_stomp_sup:start_link(Listeners, Config),
    EMPid = case rabbit_event:start_link() of
              {ok, Pid}                       -> Pid;
              {error, {already_started, Pid}} -> Pid
            end,
    gen_event:add_handler(EMPid, rabbit_stomp_internal_event_handler, []),
    Result.

stop(_) ->
    rabbit_stomp_sup:stop_listeners().

-spec close_all_client_connections(string() | binary()) -> {'ok', non_neg_integer()}.
close_all_client_connections(Reason) ->
     Connections = list(),
    [rabbit_stomp_reader:close_connection(Pid, Reason) || Pid <- Connections],
    {ok, length(Connections)}.

emit_connection_info_all(Nodes, Items, Ref, AggregatorPid) ->
    Pids = [spawn_link(Node, rabbit_stomp, emit_connection_info_local,
                       [Items, Ref, AggregatorPid])
            || Node <- Nodes],
    rabbit_control_misc:await_emitters_termination(Pids),
    ok.

emit_connection_info_local(Items, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map_with_exit_handler(
      AggregatorPid, Ref, fun(Pid) ->
                                  rabbit_stomp_reader:info(Pid, Items)
                          end,
      list()).

connection_info_local(Items) ->
    Connections = list(),
    [rabbit_stomp_reader:info(Pid, Items) || Pid <- Connections].

parse_listener_configuration() ->
    {ok, Listeners} = application:get_env(tcp_listeners),
    {ok, SslListeners} = application:get_env(ssl_listeners),
    {Listeners, SslListeners}.

parse_configuration() ->
    {ok, UserConfig} = application:get_env(default_user),
    Conf0 = parse_default_user(UserConfig, ?DEFAULT_CONFIGURATION),
    {ok, SSLLogin} = application:get_env(ssl_cert_login),
    {ok, ImplicitConnect} = application:get_env(implicit_connect),
    Conf = Conf0#stomp_configuration{ssl_cert_login   = SSLLogin,
                                     implicit_connect = ImplicitConnect},
    report_configuration(Conf),
    Conf.

parse_default_user([], Configuration) ->
    Configuration;
parse_default_user([{login, Login} | Rest], Configuration) ->
    parse_default_user(Rest, Configuration#stomp_configuration{
                               default_login = Login});
parse_default_user([{passcode, Passcode} | Rest], Configuration) ->
    parse_default_user(Rest, Configuration#stomp_configuration{
                               default_passcode = Passcode});
parse_default_user([Unknown | Rest], Configuration) ->
    _ = rabbit_log:warning("rabbit_stomp: ignoring invalid default_user "
                       "configuration option: ~p~n", [Unknown]),
    parse_default_user(Rest, Configuration).

report_configuration(#stomp_configuration{
                        default_login    = Login,
                        implicit_connect = ImplicitConnect,
                        ssl_cert_login   = SSLCertLogin}) ->
    case Login of
        undefined -> ok;
        _         -> _ = rabbit_log:info("rabbit_stomp: default user '~s' "
                                     "enabled~n", [Login])
    end,

    case ImplicitConnect of
        true  -> _ = rabbit_log:info("rabbit_stomp: implicit connect enabled~n");
        false -> ok
    end,

    case SSLCertLogin of
        true  -> _ = rabbit_log:info("rabbit_stomp: ssl_cert_login enabled~n");
        false -> ok
    end,

    ok.

list() ->
    [Client ||
        {_, ListSup, _, _} <- supervisor2:which_children(rabbit_stomp_sup),
        {_, RanchEmbeddedSup, supervisor, _} <- supervisor2:which_children(ListSup),
        {{ranch_listener_sup, _}, RanchListSup, _, _} <- supervisor:which_children(RanchEmbeddedSup),
        {ranch_conns_sup_sup, RanchConnsSup, supervisor, _} <- supervisor2:which_children(RanchListSup),
        {_, RanchConnSup, supervisor, _} <- supervisor2:which_children(RanchConnsSup),
        {_, StompClientSup, supervisor, _} <- supervisor2:which_children(RanchConnSup),
        {rabbit_stomp_reader, Client, _, _} <- supervisor:which_children(StompClientSup)].
