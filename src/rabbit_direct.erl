%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_direct).

-export([boot/0, force_event_refresh/1, list/0, connect/5,
         start_channel/10, disconnect/2]).

-deprecated([{force_event_refresh, 1, eventually}]).

%% Internal
-export([list_local/0]).

%% For testing only
-export([extract_extra_auth_props/4]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-spec boot() -> 'ok'.

boot() -> rabbit_sup:start_supervisor_child(
            rabbit_direct_client_sup, rabbit_client_sup,
            [{local, rabbit_direct_client_sup},
             {rabbit_channel_sup, start_link, []}]).

-spec force_event_refresh(reference()) -> 'ok'.

force_event_refresh(Ref) ->
    [Pid ! {force_event_refresh, Ref} || Pid <- list()],
    ok.

-spec list_local() -> [pid()].

list_local() ->
    pg_local:get_members(rabbit_direct).

-spec list() -> [pid()].

list() ->
    rabbit_misc:append_rpc_all_nodes(rabbit_mnesia:cluster_nodes(running),
                                     rabbit_direct, list_local, []).

%%----------------------------------------------------------------------------

auth_fun({none, _}, _VHost, _ExtraAuthProps) ->
    fun () -> {ok, rabbit_auth_backend_dummy:user()} end;

auth_fun({Username, none}, _VHost, _ExtraAuthProps) ->
    fun () -> rabbit_access_control:check_user_login(Username, []) end;

auth_fun({Username, Password}, VHost, ExtraAuthProps) ->
    fun () ->
        rabbit_access_control:check_user_login(
            Username,
            [{password, Password}, {vhost, VHost}] ++ ExtraAuthProps)
    end.

-spec connect
        (({'none', 'none'} | {rabbit_types:username(), 'none'} |
          {rabbit_types:username(), rabbit_types:password()}),
         rabbit_types:vhost(), rabbit_types:protocol(), pid(),
         rabbit_event:event_props()) ->
            rabbit_types:ok_or_error2(
              {rabbit_types:user(), rabbit_framing:amqp_table()},
              'broker_not_found_on_node' |
              {'auth_failure', string()} | 'access_refused').

connect(Creds, VHost, Protocol, Pid, Infos) ->
    ExtraAuthProps = extract_extra_auth_props(Creds, VHost, Pid, Infos),
    AuthFun = auth_fun(Creds, VHost, ExtraAuthProps),
    case rabbit:is_running() of
        true  ->
            case whereis(rabbit_direct_client_sup) of
                undefined ->
                    {error, broker_is_booting};
                _ ->
                    case is_over_connection_limit(VHost, Creds, Pid) of
                        true  ->
                            {error, not_allowed};
                        false ->
                            case is_vhost_alive(VHost, Creds, Pid) of
                                false ->
                                    {error, {internal_error, vhost_is_down}};
                                true  ->
                                    case AuthFun() of
                                        {ok, User = #user{username = Username}} ->
                                            notify_auth_result(Username,
                                                               user_authentication_success, []),
                                            connect1(User, VHost, Protocol, Pid, Infos);
                                        {refused, Username, Msg, Args} ->
                                            notify_auth_result(Username,
                                                               user_authentication_failure,
                                                               [{error, rabbit_misc:format(Msg, Args)}]),
                                            {error, {auth_failure, "Refused"}}
                                    end %% AuthFun()
                            end %% is_vhost_alive
                    end %% is_over_connection_limit
            end;
        false -> {error, broker_not_found_on_node}
    end.

extract_extra_auth_props(Creds, VHost, Pid, Infos) ->
    case extract_protocol(Infos) of
        undefined ->
            [];
        Protocol ->
            maybe_call_connection_info_module(Protocol, Creds, VHost, Pid, Infos)
    end.

extract_protocol(Infos) ->
    case proplists:get_value(protocol, Infos, undefined) of
        {Protocol, _Version} ->
            Protocol;
        _ ->
            undefined
    end.

maybe_call_connection_info_module(Protocol, Creds, VHost, Pid, Infos) ->
    Module = rabbit_data_coercion:to_atom(string:to_lower(
        "rabbit_" ++
        lists:flatten(string:replace(rabbit_data_coercion:to_list(Protocol), " ", "_", all)) ++
        "_connection_info")
    ),
    Args = [Creds, VHost, Pid, Infos],
    code_server_cache:maybe_call_mfa(Module, additional_authn_params, Args, []).

is_vhost_alive(VHost, {Username, _Password}, Pid) ->
    PrintedUsername = case Username of
        none -> "";
        _    -> Username
    end,
    case rabbit_vhost_sup_sup:is_vhost_alive(VHost) of
        true  -> true;
        false ->
            rabbit_log_connection:error(
                "Error on Direct connection ~p~n"
                "access to vhost '~s' refused for user '~s': "
                "vhost '~s' is down",
                [Pid, VHost, PrintedUsername, VHost]),
            false
    end.

is_over_connection_limit(VHost, {Username, _Password}, Pid) ->
    PrintedUsername = case Username of
        none -> "";
        _    -> Username
    end,
    try rabbit_vhost_limit:is_over_connection_limit(VHost) of
        false         -> false;
        {true, Limit} ->
            rabbit_log_connection:error(
                "Error on Direct connection ~p~n"
                "access to vhost '~s' refused for user '~s': "
                "connection limit (~p) is reached",
                [Pid, VHost, PrintedUsername, Limit]),
            true
    catch
        throw:{error, {no_such_vhost, VHost}} ->
            rabbit_log_connection:error(
                "Error on Direct connection ~p~n"
                "vhost ~s not found", [Pid, VHost]),
            true
    end.

notify_auth_result(Username, AuthResult, ExtraProps) ->
    EventProps = [{connection_type, direct},
                  {name, case Username of none -> ''; _ -> Username end}] ++
                 ExtraProps,
    rabbit_event:notify(AuthResult, [P || {_, V} = P <- EventProps, V =/= '']).

connect1(User, VHost, Protocol, Pid, Infos) ->
    % Note: peer_host can be either a tuple or
    % a binary if reverse_dns_lookups is enabled
    PeerHost = proplists:get_value(peer_host, Infos),
    AuthzContext = proplists:get_value(variable_map, Infos, #{}),
    try rabbit_access_control:check_vhost_access(User, VHost, {ip, PeerHost}, AuthzContext) of
        ok -> ok = pg_local:join(rabbit_direct, Pid),
	      rabbit_core_metrics:connection_created(Pid, Infos),
              rabbit_event:notify(connection_created, Infos),
              {ok, {User, rabbit_reader:server_properties(Protocol)}}
    catch
        exit:#amqp_error{name = Reason = not_allowed} ->
            {error, Reason}
    end.

-spec start_channel
        (rabbit_channel:channel_number(), pid(), pid(), string(),
         rabbit_types:protocol(), rabbit_types:user(), rabbit_types:vhost(),
         rabbit_framing:amqp_table(), pid(), any()) ->
            {'ok', pid()}.

start_channel(Number, ClientChannelPid, ConnPid, ConnName, Protocol, User,
              VHost, Capabilities, Collector, AmqpParams) ->
    {ok, _, {ChannelPid, _}} =
        supervisor2:start_child(
          rabbit_direct_client_sup,
          [{direct, Number, ClientChannelPid, ConnPid, ConnName, Protocol,
            User, VHost, Capabilities, Collector, AmqpParams}]),
    {ok, ChannelPid}.

-spec disconnect(pid(), rabbit_event:event_props()) -> 'ok'.

disconnect(Pid, Infos) ->
    pg_local:leave(rabbit_direct, Pid),
    rabbit_core_metrics:connection_closed(Pid),
    rabbit_event:notify(connection_closed, Infos).
