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
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C)
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_framing_channel).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([start/4, unexpected_message/1]).
-export([read_frame/0, read_method/0, read_method/1]).
-export([collect_content/1]).

-define(CLOSING_TIMEOUT, 10).

%%--------------------------------------------------------------------

start(ReaderPid, Channel, Sock, Connection) ->
    {ok, _, _} = read_method('channel.open'),
    rabbit_writer:internal_send_command(Sock, Channel, #'channel.open_ok'{}),
    Tx0 = rabbit_transaction:start(),
    WriterPid = rabbit_writer:start(Sock, Channel, Connection, Tx0),
    Tx1 = rabbit_transaction:set_writer_pid(Tx0, WriterPid),
    mainloop(#ch{ channel = Channel,
                  tx = Tx1,
                  reader_pid = ReaderPid,
                  writer_pid = WriterPid,
                  username = (Connection#connection.user)#user.username,
                  virtual_host = Connection#connection.vhost,
                  most_recently_declared_queue = <<>>,
                  consumer_mapping = dict:new(),
                  next_ticket = 101 }).

unexpected_message(Msg) ->
    rabbit_log:error("Channel received unexpected message: ~p~n", [Msg]),
    exit({unexpected_channel_message, Msg}).

read_frame() ->
    receive
        terminate ->
            %% doing this, rather than terminating normally, ensures
            %% that our linked processes exit too
            exit(terminating);
        {force_termination, Channel} ->
            rabbit_log:warning("forcing termination of channel ~p~n",
                               [Channel]),
            exit(normal);
        {frame, _Channel, Frame} -> Frame;
        Other -> unexpected_message(Other)
    end.

read_method() ->
    {method, Method, Fields} = read_frame(),
    finish_reading_method(Method, Fields).

read_method(ExpectedMethodName) ->
    {method, Method, Fields} = read_frame(),
    if
        Method == ExpectedMethodName ->
            finish_reading_method(Method, Fields);
        true ->
            rabbit_misc:die(channel_error, Method)
    end.

%%--------------------------------------------------------------------

mainloop(State = #ch{channel = Channel}) ->
    {ok, MethodRecord, Content} = read_method(),
    case rabbit_channel:safe_handle_method(MethodRecord, Content, State) of
        stop -> ok;
        terminate ->
            erlang:send_after(?CLOSING_TIMEOUT * 1000, self(),
                              {force_termination, Channel}),
            termination_loop(Channel);
        NewState -> mainloop(NewState)
    end.

%% We only get here after telling the reader about an exception we
%% encountered when handling a method. Depending on the error, the
%% reader will either send a channel.close or a connection.close.
%% According to the spec we must ignore all frames except
%% channel.close_ok after the former has been sent. It's also safe to
%% do so in the latter case.
termination_loop(Channel) ->
    case read_frame() of
        {method, 'channel.close_ok', _} ->
            ok;
        Frame ->
            rabbit_log:warning("ignoring frame ~p on closed channel ~p~n",
                               [Frame, Channel]),
            termination_loop(Channel)
    end.

collect_content(MethodName) ->
    {ClassId, _MethodId} = rabbit_framing:method_id(MethodName),
    case read_frame() of
        {content_header, HeaderClassId, 0, BodySize, PropertiesBin}
        when HeaderClassId == ClassId ->
            #content{class_id = ClassId,
                     properties = none,
                     properties_bin = PropertiesBin,
                     payload_fragments_rev = collect_content_payload(BodySize, [])};
        _ -> rabbit_misc:die(frame_error)
    end.

collect_content_payload(0, Acc) ->
    Acc;
collect_content_payload(RemainingByteCount, Acc) ->
    case read_frame() of
        {content_body, FragmentBin} ->
            collect_content_payload(RemainingByteCount - size(FragmentBin), [FragmentBin | Acc]);
        _ -> rabbit_misc:die(frame_error)
    end.

%%finish_reading_method(MethodName, FieldsBin) ->
%%    X = {ok, MethodRecord, Content} = finish_reading_method1(MethodName, FieldsBin),
%%    rabbit_log:message(in, "??", MethodRecord, Content),
%%    X.
finish_reading_method(MethodName, FieldsBin) ->
    {ok,
     rabbit_framing:decode_method_fields(MethodName, FieldsBin),
     case rabbit_framing:method_has_content(MethodName) of
         true ->
             case catch collect_content(MethodName) of
                 {'EXIT', Reason} ->
                     rabbit_log:info("Syntax error collecting method content: ~p~n", [Reason]),
                     rabbit_misc:die(syntax_error, MethodName);
                 Result -> Result
             end;
         false ->
             none
     end}.

