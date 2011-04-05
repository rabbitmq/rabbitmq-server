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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_log).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([debug/1, debug/2, message/4, info/1, info/2,
         warning/1, warning/2, error/1, error/2]).

-export([tap_trace_in/2, tap_trace_out/3]).

-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-define(SERVER, ?MODULE).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
-spec(debug/1 :: (string()) -> 'ok').
-spec(debug/2 :: (string(), [any()]) -> 'ok').
-spec(info/1 :: (string()) -> 'ok').
-spec(info/2 :: (string(), [any()]) -> 'ok').
-spec(warning/1 :: (string()) -> 'ok').
-spec(warning/2 :: (string(), [any()]) -> 'ok').
-spec(error/1 :: (string()) -> 'ok').
-spec(error/2 :: (string(), [any()]) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

debug(Fmt) ->
    gen_server:cast(?SERVER, {debug, Fmt}).

debug(Fmt, Args) when is_list(Args) ->
    gen_server:cast(?SERVER, {debug, Fmt, Args}).

message(Direction, Channel, MethodRecord, Content) ->
    gen_server:cast(?SERVER,
                    {message, Direction, Channel, MethodRecord, Content}).

info(Fmt) ->
    gen_server:cast(?SERVER, {info, Fmt}).

info(Fmt, Args) when is_list(Args) ->
    gen_server:cast(?SERVER, {info, Fmt, Args}).

warning(Fmt) ->
    gen_server:cast(?SERVER, {warning, Fmt}).

warning(Fmt, Args) when is_list(Args) ->
    gen_server:cast(?SERVER, {warning, Fmt, Args}).

error(Fmt) ->
    gen_server:cast(?SERVER, {error, Fmt}).

error(Fmt, Args) when is_list(Args) ->
    gen_server:cast(?SERVER, {error, Fmt, Args}).

tap_trace_in(Message = #basic_message{exchange_name = #resource{
                                        virtual_host = VHostBin,
                                        name = XNameBin}},
             QPids) ->
    check_trace(
      VHostBin,
      fun (TraceExchangeBin) ->
              QInfos = [rabbit_amqqueue:info(#amqqueue{pid = P}, [name]) ||
                           P <- QPids],
              QNames = [N || [{name, #resource{name = N}}] <- QInfos],
              QNamesStr = list_to_binary(rabbit_misc:intersperse(",", QNames)),
              EncodedMessage = message_to_table(Message),
              maybe_inject(TraceExchangeBin, VHostBin, XNameBin,
                           <<"publish">>, XNameBin,
                           [{<<"queue_names">>, longstr, QNamesStr},
                            {<<"message">>, table, EncodedMessage}])
      end).

tap_trace_out({#resource{name = QNameBin}, _QPid, QMsgId, Redelivered,
               Message = #basic_message{exchange_name = #resource{
                                          virtual_host = VHostBin,
                                          name = XNameBin}}},
              DeliveryTag,
              ConsumerTagOrNone) ->
    check_trace(
      VHostBin,
      fun (TraceExchangeBin) ->
              RedeliveredNum = case Redelivered of true -> 1; false -> 0 end,
              EncodedMessage = message_to_table(Message),
              Fields0 = [{<<"delivery_tag">>,     signedint, DeliveryTag}, %% FIXME later
                         {<<"queue_msg_number">>, signedint, QMsgId},
                         {<<"redelivered">>,      signedint, RedeliveredNum},
                         {<<"message">>,          table,     EncodedMessage}],
              Fields = case ConsumerTagOrNone of
                           none ->
                               Fields0;
                           ConsumerTag ->
                               [{<<"consumer_tag">>, longstr, ConsumerTag}
                                | Fields0]
                       end,
              maybe_inject(TraceExchangeBin, VHostBin, XNameBin,
                           <<"deliver">>, QNameBin, Fields)
      end).

check_trace(VHostBin, F) ->
    case catch case application:get_env(rabbit, {trace_exchange, VHostBin}) of
                   undefined              -> ok;
                   {ok, TraceExchangeBin} -> F(TraceExchangeBin)
               end of
        {'EXIT', Reason} -> info("Trace tap died with reason ~p~n", [Reason]);
        ok               -> ok
    end.

maybe_inject(TraceExchangeBin, VHostBin, OriginalExchangeBin,
             RKPrefix, RKSuffix, Table) ->
    if
        TraceExchangeBin =:= OriginalExchangeBin ->
            ok;
        true ->
            rabbit_exchange:simple_publish(
              false,
              false,
              rabbit_misc:r(VHostBin, exchange, TraceExchangeBin),
              <<RKPrefix/binary, ".", RKSuffix/binary>>,
              <<"application/x-amqp-table; version=0-8">>,
              rabbit_binary_generator:generate_table(Table)),
            ok
    end.

message_to_table(#basic_message{exchange_name = #resource{name = XName},
                                routing_keys = RoutingKeys,
                                content = Content}) ->
    #content{properties = #'P_basic'{content_type     = ContentType,
                                     content_encoding = ContentEncoding,
                                     headers          = Headers,
                                     delivery_mode    = DeliveryMode,
                                     priority         = Priority,
                                     correlation_id   = CorrelationId,
                                     reply_to         = ReplyTo,
                                     expiration       = Expiration,
                                     message_id       = MessageId,
                                     timestamp        = Timestamp,
                                     type             = Type,
                                     user_id          = UserId,
                                     app_id           = AppId},
             payload_fragments_rev = PFR} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    Headers = prune_undefined(
                [{<<"content_type">>,     longstr,   ContentType},
                 {<<"content_encoding">>, longstr,   ContentEncoding},
                 {<<"headers">>,          table,     Headers},
                 {<<"delivery_mode">>,    signedint, DeliveryMode},
                 {<<"priority">>,         signedint, Priority},
                 {<<"correlation_id">>,   longstr,   CorrelationId},
                 {<<"reply_to">>,         longstr,   ReplyTo},
                 {<<"expiration">>,       longstr,   Expiration},
                 {<<"message_id">>,       longstr,   MessageId},
                 {<<"timestamp">>,        longstr,   Timestamp},
                 {<<"type">>,             longstr,   Type},
                 {<<"user_id">>,          longstr,   UserId},
                 {<<"app_id">>,           longstr,   AppId}]),
    [{<<"exchange_name">>, longstr, XName},
     {<<"routing_key">>,   array,   [{longstr, K} || K <- RoutingKeys]},
     {<<"headers">>,       table,   Headers},
     {<<"body">>,          longstr, list_to_binary(lists:reverse(PFR))}].

prune_undefined(Fields) ->
    [F || F = {_, _, Value} <- Fields,
          Value =/= undefined].

%%--------------------------------------------------------------------

init([]) -> {ok, none}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({debug, Fmt}, State) ->
    io:format("debug:: "), io:format(Fmt),
    error_logger:info_msg("debug:: " ++ Fmt),
    {noreply, State};
handle_cast({debug, Fmt, Args}, State) ->
    io:format("debug:: "), io:format(Fmt, Args),
    error_logger:info_msg("debug:: " ++ Fmt, Args),
    {noreply, State};
handle_cast({message, Direction, Channel, MethodRecord, Content}, State) ->
    io:format("~s ch~p ~p~n",
              [case Direction of
                   in -> "-->";
                   out -> "<--" end,
               Channel,
               {MethodRecord, Content}]),
    {noreply, State};
handle_cast({info, Fmt}, State) ->
    error_logger:info_msg(Fmt),
    {noreply, State};
handle_cast({info, Fmt, Args}, State) ->
    error_logger:info_msg(Fmt, Args),
    {noreply, State};
handle_cast({warning, Fmt}, State) ->
    error_logger:warning_msg(Fmt),
    {noreply, State};
handle_cast({warning, Fmt, Args}, State) ->
    error_logger:warning_msg(Fmt, Args),
    {noreply, State};
handle_cast({error, Fmt}, State) ->
    error_logger:error_msg(Fmt),
    {noreply, State};
handle_cast({error, Fmt, Args}, State) ->
    error_logger:error_msg(Fmt, Args),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

