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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_federation_link_util).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

%% real
-export([start_conn_ch/5, disposable_channel_call/2, disposable_channel_call/3,
         disposable_connection_call/3, ensure_connection_closed/1,
         log_terminate/4, unacked_new/0, ack/3, nack/3, forward/9,
         handle_down/6, get_connection_name/2,
         log_debug/3, log_info/3, log_warning/3, log_error/3]).

%% temp
-export([connection_error/6]).

-import(rabbit_misc, [pget/2]).

-define(MAX_CONNECTION_CLOSE_TIMEOUT, 10000).

%%----------------------------------------------------------------------------

start_conn_ch(Fun, Upstream, UParams,
              XorQName = #resource{virtual_host = DownVHost}, State) ->
    ConnName = get_connection_name(Upstream, UParams),
    case open_monitor(#amqp_params_direct{virtual_host = DownVHost}, ConnName) of
        {ok, DConn, DCh} ->
            case Upstream#upstream.ack_mode of
                'on-confirm' ->
                    #'confirm.select_ok'{} =
                        amqp_channel:call(DCh, #'confirm.select'{}),
                    amqp_channel:register_confirm_handler(DCh, self());
                _ ->
                    ok
            end,
            case open_monitor(UParams#upstream_params.params, ConnName) of
                {ok, Conn, Ch} ->
                    %% Don't trap exits until we have established
                    %% connections so that if we try to delete
                    %% federation upstreams while waiting for a
                    %% connection to be established then we don't
                    %% block
                    process_flag(trap_exit, true),
                    try
                        R = Fun(Conn, Ch, DConn, DCh),
                        log_info(
                          XorQName, "connected to ~s~n",
                          [rabbit_federation_upstream:params_to_string(
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

get_connection_name(#upstream{name = UpstreamName},
    #upstream_params{x_or_q = Resource}) when is_record(Resource, exchange)->
    Policy = Resource#exchange.policy,
    PolicyName = proplists:get_value(name, Policy),
    connection_name(UpstreamName, PolicyName);

get_connection_name(#upstream{name = UpstreamName},
    #upstream_params{x_or_q = Resource}) when is_record(Resource, amqqueue)->
    Policy = Resource#amqqueue.policy,
    PolicyName = proplists:get_value(name, Policy),
    connection_name(UpstreamName, PolicyName);

get_connection_name(_, _) ->
    connection_name(undefined, undefined).

connection_name(Upstream, Policy) when is_binary(Upstream), is_binary(Policy) ->
    <<<<"Federation link (upstream: ">>/binary, Upstream/binary, <<", policy: ">>/binary, Policy/binary, <<")">>/binary>>;

connection_name(_, _) ->
    <<"Federation link">>.

open_monitor(Params, Name) ->
    case open(Params, Name) of
        {ok, Conn, Ch} -> erlang:monitor(process, Ch),
                          {ok, Conn, Ch};
        E              -> E
    end.

open(Params, Name) ->
    case amqp_connection:start(Params, Name) of
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
    log_warning(XorQName, "did not connect to ~s. Reason: ~p~n",
                [rabbit_federation_upstream:params_to_string(UParams),
                 E]),
    {stop, {shutdown, restart}, State};

connection_error(remote, E, Upstream, UParams, XorQName, State) ->
    rabbit_federation_status:report(
      Upstream, UParams, XorQName, clean_reason(E)),
    log_info(XorQName, "disconnected from ~s~n~p~n",
             [rabbit_federation_upstream:params_to_string(UParams), E]),
    {stop, {shutdown, restart}, State};

connection_error(local, basic_cancel, Upstream, UParams, XorQName, State) ->
    rabbit_federation_status:report(
      Upstream, UParams, XorQName, {error, basic_cancel}),
    log_info(XorQName, "received 'basic.cancel'~n", []),
    {stop, {shutdown, restart}, State};

connection_error(local_start, E, Upstream, UParams, XorQName, State) ->
    rabbit_federation_status:report(
      Upstream, UParams, XorQName, clean_reason(E)),
    log_warning(XorQName, "did not connect locally~n~p~n", [E]),
    {stop, {shutdown, restart}, State}.

%% If we terminate due to a gen_server call exploding (almost
%% certainly due to an amqp_channel:call() exploding) then we do not
%% want to report the gen_server call in our status.
clean_reason({E = {shutdown, _}, _}) -> E;
clean_reason(E)                      -> E.

%% local / disconnected never gets invoked, see handle_info({'DOWN', ...

%%----------------------------------------------------------------------------

unacked_new() -> gb_trees:empty().

ack(#'basic.ack'{delivery_tag = Seq,
                 multiple     = Multiple}, Ch, Unack) ->
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = gb_trees:get(Seq, Unack),
                                       multiple     = Multiple}),
    remove_delivery_tags(Seq, Multiple, Unack).


%% Note: at time of writing the broker will never send requeue=false. And it's
%% hard to imagine why it would. But we may as well handle it.
nack(#'basic.nack'{delivery_tag = Seq,
                   multiple     = Multiple,
                   requeue      = Requeue}, Ch, Unack) ->
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = gb_trees:get(Seq, Unack),
                                        multiple     = Multiple,
                                        requeue      = Requeue}),
    remove_delivery_tags(Seq, Multiple, Unack).

remove_delivery_tags(Seq, false, Unacked) ->
    gb_trees:delete(Seq, Unacked);
remove_delivery_tags(Seq, true, Unacked) ->
    case gb_trees:is_empty(Unacked) of
        true  -> Unacked;
        false -> {Smallest, _Val, Unacked1} = gb_trees:take_smallest(Unacked),
                 case Smallest > Seq of
                     true  -> Unacked;
                     false -> remove_delivery_tags(Seq, true, Unacked1)
                 end
    end.

forward(#upstream{ack_mode      = AckMode,
                  trust_user_id = Trust},
        #'basic.deliver'{delivery_tag = DT},
        Ch, DCh, PublishMethod, HeadersFun, ForwardFun, Msg, Unacked) ->
    Headers = extract_headers(Msg),
    case ForwardFun(Headers) of
        true  -> Msg1 = maybe_clear_user_id(
                          Trust, update_headers(HeadersFun(Headers), Msg)),
                 Seq = case AckMode of
                           'on-confirm' -> amqp_channel:next_publish_seqno(DCh);
                           _            -> ignore
                       end,
                 amqp_channel:cast(DCh, PublishMethod, Msg1),
                 case AckMode of
                     'on-confirm' ->
                         gb_trees:insert(Seq, DT, Unacked);
                     'on-publish' ->
                         amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DT}),
                         Unacked;
                     'no-ack' ->
                         Unacked
                 end;
        false -> amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DT}),
                 %% Drop it, but acknowledge it!
                 Unacked
    end.

maybe_clear_user_id(false, Msg = #amqp_msg{props = Props}) ->
    Msg#amqp_msg{props = Props#'P_basic'{user_id = undefined}};
maybe_clear_user_id(true, Msg) ->
    Msg.

extract_headers(#amqp_msg{props = #'P_basic'{headers = Headers}}) ->
    Headers.

update_headers(Headers, Msg = #amqp_msg{props = Props}) ->
    Msg#amqp_msg{props = Props#'P_basic'{headers = Headers}}.

%%----------------------------------------------------------------------------

%% If the downstream channel shuts down cleanly, we can just ignore it
%% - we're the same node, we're presumably about to go down too.
handle_down(DCh, shutdown, _Ch, DCh, _Args, State) ->
    {noreply, State};

%% If the upstream channel goes down for an intelligible reason, just
%% log it and die quietly.
handle_down(Ch, {shutdown, Reason}, Ch, _DCh,
            {Upstream, UParams, XName}, State) ->
    rabbit_federation_link_util:connection_error(
      remote, {upstream_channel_down, Reason}, Upstream, UParams, XName, State);

handle_down(Ch, Reason, Ch, _DCh, _Args, State) ->
    {stop, {upstream_channel_down, Reason}, State};

handle_down(DCh, Reason, _Ch, DCh, _Args, State) ->
    {stop, {downstream_channel_down, Reason}, State}.

%%----------------------------------------------------------------------------

log_terminate({shutdown, restart}, _Upstream, _UParams, _XorQName) ->
    %% We've already logged this before munging the reason
    ok;
log_terminate(shutdown, Upstream, UParams, XorQName) ->
    %% The supervisor is shutting us down; we are probably restarting
    %% the link because configuration has changed. So try to shut down
    %% nicely so that we do not cause unacked messages to be
    %% redelivered.
    log_info(XorQName, "disconnecting from ~s~n",
             [rabbit_federation_upstream:params_to_string(UParams)]),
    rabbit_federation_status:remove(Upstream, XorQName);

log_terminate(Reason, Upstream, UParams, XorQName) ->
    %% Unexpected death. sasl will log it, but we should update
    %% rabbit_federation_status.
    rabbit_federation_status:report(
      Upstream, UParams, XorQName, clean_reason(Reason)).

log_debug(XorQName, Fmt, Args) -> log(debug, XorQName, Fmt, Args).
log_info(XorQName, Fmt, Args) -> log(info, XorQName, Fmt, Args).
log_warning(XorQName, Fmt, Args) -> log(warning, XorQName, Fmt, Args).
log_error(XorQName, Fmt, Args) -> log(error, XorQName, Fmt, Args).

log(Level, XorQName, Fmt0, Args0) ->
    Fmt = "Federation ~s " ++ Fmt0,
    Args = [rabbit_misc:rs(XorQName) | Args0],
    case Level of
        debug   -> rabbit_log_federation:debug(Fmt, Args);
        info    -> rabbit_log_federation:info(Fmt, Args);
        warning -> rabbit_log_federation:warning(Fmt, Args);
        error   -> rabbit_log_federation:error(Fmt, Args)
    end.

%%----------------------------------------------------------------------------

disposable_channel_call(Conn, Method) ->
    disposable_channel_call(Conn, Method, fun(_, _) -> ok end).

disposable_channel_call(Conn, Method, ErrFun) ->
    try
        {ok, Ch} = amqp_connection:open_channel(Conn),
        try
            amqp_channel:call(Ch, Method)
        catch exit:{{shutdown, {server_initiated_close, Code, Text}}, _} ->
                ErrFun(Code, Text)
        after
            ensure_channel_closed(Ch)
        end
    catch
          Exception:Reason ->
            rabbit_log_federation:error("Federation link could not create a disposable (one-off) channel due to an error ~p: ~p~n", [Exception, Reason])
    end.

disposable_connection_call(Params, Method, ErrFun) ->
    case open(Params, undefined) of
        {ok, Conn, Ch} ->
            try
                amqp_channel:call(Ch, Method)
            catch exit:{{shutdown, {connection_closing,
                                    {server_initiated_close, Code, Txt}}}, _} ->
                    ErrFun(Code, Txt);
                    exit:{{shutdown, {server_initiated_close, Code, Txt}}, _} ->
                    ErrFun(Code, Txt)
            after
                ensure_connection_closed(Conn)
            end;
        E ->
            E
    end.
