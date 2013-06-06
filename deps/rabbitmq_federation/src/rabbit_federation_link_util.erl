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
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_link_util).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

%% real
-export([start_conn_ch/5, disposable_channel_call/2, disposable_channel_call/3,
         disposable_connection_call/3, ensure_connection_closed/1,
         log_terminate/4]).

%% temp
-export([connection_error/6]).

-import(rabbit_misc, [pget/2]).

-define(MAX_CONNECTION_CLOSE_TIMEOUT, 10000).

%%----------------------------------------------------------------------------

start_conn_ch(Fun, Upstream, UParams,
              XorQName = #resource{virtual_host = DownVHost}, State) ->
    %% We trap exits so terminate/2 gets called. Note that this is not
    %% in init() since we need to cope with the link getting restarted
    %% during shutdown (when a broker federates with itself), which
    %% means we hang in federation_up() and the supervisor must force
    %% us to exit. We can therefore only trap exits when past that
    %% point. Bug 24372 may help us do something nicer.
    process_flag(trap_exit, true),
    case open_monitor(local_params(Upstream, DownVHost)) of
        {ok, DConn, DCh} ->
            case Upstream#upstream.ack_mode of
                'on-confirm' ->
                    #'confirm.select_ok'{} =
                        amqp_channel:call(DCh, #'confirm.select'{}),
                    amqp_channel:register_confirm_handler(DCh, self());
                _ ->
                    ok
            end,
            case open_monitor(UParams#upstream_params.params) of
                {ok, Conn, Ch} ->
                    try
                        R = Fun(Conn, Ch, DConn, DCh),
                        rabbit_log:info(
                          "Federation ~s connected to ~s~n",
                          [rabbit_misc:rs(XorQName),
                           rabbit_federation_upstream:params_to_string(
                             UParams)]),
                        Name = pget(name, amqp_connection:info(DConn, [name])),
                        rabbit_federation_status:report(
                          Upstream, UParams, XorQName, {running, Name}),
                        R
                    catch exit:E ->
                            %% terminate/2 will not get this, as we
                            %% have not put them in our state yet
                            ensure_connection_closed(DConn),
                            ensure_connection_closed(Conn),
                            connection_error(remote_start, E,
                                             Upstream, UParams, XorQName, State)
                    end;
                E ->
                    ensure_connection_closed(DConn),
                    connection_error(remote_start, E,
                                     Upstream, UParams, XorQName, State)
            end;
        E ->
            connection_error(local_start, E,
                             Upstream, UParams, XorQName, State)
    end.

open_monitor(Params) ->
    case open(Params) of
        {ok, Conn, Ch} -> erlang:monitor(process, Ch),
                          {ok, Conn, Ch};
        E              -> E
    end.

open(Params) ->
    case amqp_connection:start(Params) of
        {ok, Conn} -> case amqp_connection:open_channel(Conn) of
                          {ok, Ch} -> {ok, Conn, Ch};
                          E        -> catch amqp_connection:close(Conn),
                                      E
                      end;
        E -> E
    end.

ensure_channel_closed(Ch) -> catch amqp_channel:close(Ch).

ensure_connection_closed(Conn) ->
    catch amqp_connection:close(Conn, ?MAX_CONNECTION_CLOSE_TIMEOUT).

connection_error(remote_start, E, Upstream, UParams, XorQName, State) ->
    rabbit_federation_status:report(
      Upstream, UParams, XorQName, clean_reason(E)),
    rabbit_log:warning("Federation ~s did not connect to ~s~n~p~n",
                       [rabbit_misc:rs(XorQName),
                        rabbit_federation_upstream:params_to_string(UParams),
                        E]),
    {stop, {shutdown, restart}, State};

connection_error(remote, E, Upstream, UParams, XorQName, State) ->
    rabbit_federation_status:report(
      Upstream, UParams, XorQName, clean_reason(E)),
    rabbit_log:info("Federation ~s disconnected from ~s~n~p~n",
                    [rabbit_misc:rs(XorQName),
                     rabbit_federation_upstream:params_to_string(UParams), E]),
    {stop, {shutdown, restart}, State};

connection_error(local, basic_cancel, Upstream, UParams, XorQName, State) ->
    rabbit_federation_status:report(
      Upstream, UParams, XorQName, {error, basic_cancel}),
    rabbit_log:info("Federation ~s received 'basic.cancel'~n",
                    [rabbit_misc:rs(XorQName)]),
    {stop, {shutdown, restart}, State};

connection_error(local_start, E, Upstream, UParams, XorQName, State) ->
    rabbit_federation_status:report(
      Upstream, UParams, XorQName, clean_reason(E)),
    rabbit_log:warning("Federation ~s did not connect locally~n~p~n",
                       [rabbit_misc:rs(XorQName), E]),
    {stop, {shutdown, restart}, State}.

%% If we terminate due to a gen_server call exploding (almost
%% certainly due to an amqp_channel:call() exploding) then we do not
%% want to report the gen_server call in our status.
clean_reason({E = {shutdown, _}, _}) -> E;
clean_reason(E)                      -> E.

%% local / disconnected never gets invoked, see handle_info({'DOWN', ...

%%----------------------------------------------------------------------------

log_terminate({shutdown, restart}, _Upstream, _UParams, _XorQName) ->
    %% We've already logged this before munging the reason
    ok;
log_terminate(shutdown, Upstream, UParams, XorQName) ->
    %% The supervisor is shutting us down; we are probably restarting
    %% the link because configuration has changed. So try to shut down
    %% nicely so that we do not cause unacked messages to be
    %% redelivered.
    rabbit_log:info("Federation ~s disconnecting from ~s~n",
                    [rabbit_misc:rs(XorQName),
                     rabbit_federation_upstream:params_to_string(UParams)]),
    rabbit_federation_status:remove(Upstream, XorQName);

log_terminate(Reason, Upstream, UParams, XorQName) ->
    %% Unexpected death. sasl will log it, but we should update
    %% rabbit_federation_status.
    rabbit_federation_status:report(
      Upstream, UParams, XorQName, clean_reason(Reason)).

%%----------------------------------------------------------------------------

disposable_channel_call(Conn, Method) ->
    disposable_channel_call(Conn, Method, fun(_, _) -> ok end).

disposable_channel_call(Conn, Method, ErrFun) ->
    {ok, Ch} = amqp_connection:open_channel(Conn),
    try
        amqp_channel:call(Ch, Method)
    catch exit:{{shutdown, {server_initiated_close, Code, Text}}, _} ->
            ErrFun(Code, Text)
    after
        ensure_channel_closed(Ch)
    end.

disposable_connection_call(Params, Method, ErrFun) ->
    case open(Params) of
        {ok, Conn, Ch} ->
            try
                amqp_channel:call(Ch, Method)
            catch exit:{{shutdown, {connection_closing,
                                    {server_initiated_close, Code, Txt}}}, _} ->
                    ErrFun(Code, Txt)
            after
                ensure_connection_closed(Conn)
            end;
        E ->
            E
    end.

local_params(#upstream{trust_user_id = Trust}, VHost) ->
    {ok, DefaultUser} = application:get_env(rabbit, default_user),
    Username = rabbit_runtime_parameters:value(
                 VHost, <<"federation">>, <<"local-username">>, DefaultUser),
    case rabbit_access_control:check_user_login(Username, []) of
        {ok, User0}        -> User = maybe_impersonator(Trust, User0),
                              #amqp_params_direct{username     = User,
                                                  virtual_host = VHost};
        {refused, _M, _A}  -> exit({error, user_does_not_exist})
    end.

maybe_impersonator(Trust, User = #user{tags = Tags}) ->
    case Trust andalso not lists:member(impersonator, Tags) of
        true  -> User#user{tags = [impersonator | Tags]};
        false -> User
    end.
