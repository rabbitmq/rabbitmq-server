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
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_amqp1_0_writer).
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-export([start/5, start_link/5, mainloop/2, mainloop1/2]).
-export([send_command/2, send_command/3, send_command_sync/2,
         send_command_sync/3, send_command_and_notify/5]).
-export([internal_send_command/4, internal_send_command/6]).

-import(gen_tcp).

-record(wstate, {sock, channel, frame_max, protocol}).

-define(HIBERNATE_AFTER, 5000).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start/5 ::
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid())
        -> rabbit_types:ok(pid())).
-spec(start_link/5 ::
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid())
        -> rabbit_types:ok(pid())).
-spec(send_command/2 ::
        (pid(), rabbit_framing:amqp_method_record()) -> 'ok').
-spec(send_command/3 ::
        (pid(), rabbit_framing:amqp_method_record(), rabbit_types:content())
        -> 'ok').
-spec(send_command_sync/2 ::
        (pid(), rabbit_framing:amqp_method_record()) -> 'ok').
-spec(send_command_sync/3 ::
        (pid(), rabbit_framing:amqp_method_record(), rabbit_types:content())
        -> 'ok').
-spec(send_command_and_notify/5 ::
        (pid(), pid(), pid(), rabbit_framing:amqp_method_record(),
         rabbit_types:content())
        -> 'ok').
-spec(internal_send_command/4 ::
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         rabbit_framing:amqp_method_record(), rabbit_types:protocol())
        -> 'ok').
-spec(internal_send_command/6 ::
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         rabbit_framing:amqp_method_record(), rabbit_types:content(),
         non_neg_integer(), rabbit_types:protocol())
        -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start(Sock, Channel, FrameMax, Protocol, ReaderPid) ->
    {ok,
     proc_lib:spawn(?MODULE, mainloop, [ReaderPid,
                                        #wstate{sock = Sock,
                                                channel = Channel,
                                                frame_max = FrameMax,
                                                protocol = Protocol}])}.

start_link(Sock, Channel, FrameMax, Protocol, ReaderPid) ->
    {ok,
     proc_lib:spawn_link(?MODULE, mainloop, [ReaderPid,
                                             #wstate{sock = Sock,
                                                     channel = Channel,
                                                     frame_max = FrameMax,
                                                     protocol = Protocol}])}.

mainloop(ReaderPid, State) ->
    try
        mainloop1(ReaderPid, State)
    catch
        exit:Error -> ReaderPid ! {channel_exit, #wstate.channel, Error}
    end,
    done.

mainloop1(ReaderPid, State) ->
    receive
        Message -> ?MODULE:mainloop1(ReaderPid, handle_message(Message, State))
    after ?HIBERNATE_AFTER ->
            erlang:hibernate(?MODULE, mainloop, [ReaderPid, State])
    end.

handle_message({send_command, MethodRecord}, State) ->
    ok = internal_send_command_async(MethodRecord, State),
    State;
handle_message({send_command, MethodRecord, Content}, State) ->
    ok = internal_send_command_async(MethodRecord, Content, State),
    State;
handle_message({'$gen_call', From, {send_command_sync, MethodRecord}}, State) ->
    ok = internal_send_command_async(MethodRecord, State),
    gen_server:reply(From, ok),
    State;
handle_message({'$gen_call', From, {send_command_sync, MethodRecord, Content}},
               State) ->
    ok = internal_send_command_async(MethodRecord, Content, State),
    gen_server:reply(From, ok),
    State;
handle_message({send_command_and_notify, QPid, ChPid, MethodRecord, Content},
               State) ->
    ok = internal_send_command_async(MethodRecord, Content, State),
    rabbit_amqqueue:notify_sent(QPid, ChPid),
    State;
handle_message({inet_reply, _, ok}, State) ->
    State;
handle_message({inet_reply, _, Status}, _State) ->
    exit({writer, send_failed, Status});
handle_message(Message, _State) ->
    exit({writer, message_not_understood, Message}).

%---------------------------------------------------------------------------

send_command(W, MethodRecord) ->
    W ! {send_command, MethodRecord},
    ok.

send_command(W, MethodRecord, Content) ->
    W ! {send_command, MethodRecord, Content},
    ok.

send_command_sync(W, MethodRecord) ->
    call(W, {send_command_sync, MethodRecord}).

send_command_sync(W, MethodRecord, Content) ->
    call(W, {send_command_sync, MethodRecord, Content}).

send_command_and_notify(W, Q, ChPid, MethodRecord, Content) ->
    W ! {send_command_and_notify, Q, ChPid, MethodRecord, Content},
    ok.

%---------------------------------------------------------------------------

call(Pid, Msg) ->
    {ok, Res} = gen:call(Pid, '$gen_call', Msg, infinity),
    Res.

%---------------------------------------------------------------------------

assemble_frames(Channel, FrameRecords, rabbit_amqp1_0_framing)
  when is_list(FrameRecords) ->
    ?LOGMESSAGE(out, Channel, FrameRecords, none),
    FrameBin = [rabbit_amqp1_0_binary_generator:generate(
                  rabbit_amqp1_0_framing:encode(F)) || F <- FrameRecords],
    rabbit_amqp1_0_binary_generator:build_frame(Channel, FrameBin);
assemble_frames(Channel, FrameRecord, rabbit_amqp1_0_framing) ->
    assemble_frames(Channel, [FrameRecord], rabbit_amqp1_0_framing);
assemble_frames(Channel, MethodRecord, Protocol) ->
    ?LOGMESSAGE(out, Channel, MethodRecord, none),
    rabbit_binary_generator:build_simple_method_frame(
      Channel, MethodRecord, Protocol).

%% Note: no (direct) AMQP 1.0 equivalent of content frames, a transfer
%% record can be followed by a number of other records to make a
%% complete frame but it's less predictable so we just model that as a
%% sequence of records going into assemble_frames/3 above.
assemble_frames(Channel, MethodRecord, Content, FrameMax, Protocol) ->
    ?LOGMESSAGE(out, Channel, MethodRecord, Content),
    MethodName = rabbit_misc:method_record_type(MethodRecord),
    true = Protocol:method_has_content(MethodName), % assertion
    MethodFrame = rabbit_binary_generator:build_simple_method_frame(
                    Channel, MethodRecord, Protocol),
    ContentFrames = rabbit_binary_generator:build_simple_content_frames(
                      Channel, Content, FrameMax, Protocol),
    [MethodFrame | ContentFrames].

tcp_send(Sock, [DM, CH, C | Cs]) ->
    rabbit_misc:throw_on_error(inet_error,
                               fun () ->
                                       Res = rabbit_net:send(Sock, [DM, CH, C]),
                                       lists:foldl(fun(_, F) ->
                                                           rabbit_net:send(Sock, F)
                                                   end, Res, Cs)
                               end);
tcp_send(Sock, Data) ->
    rabbit_misc:throw_on_error(inet_error,
                               fun () -> rabbit_net:send(Sock, Data) end).

internal_send_command(Sock, Channel, MethodRecord, Protocol) ->
    ok = tcp_send(Sock, assemble_frames(Channel, MethodRecord, Protocol)).

internal_send_command(Sock, Channel, MethodRecord, Content, FrameMax,
                      Protocol) ->
    ok = tcp_send(Sock, assemble_frames(Channel, MethodRecord,
                                        Content, FrameMax, Protocol)).

%% gen_tcp:send/2 does a selective receive of {inet_reply, Sock,
%% Status} to obtain the result. That is bad when it is called from
%% the writer since it requires scanning of the writers possibly quite
%% large message queue.
%%
%% So instead we lift the code from prim_inet:send/2, which is what
%% gen_tcp:send/2 calls, do the first half here and then just process
%% the result code in handle_message/2 as and when it arrives.
%%
%% This means we may end up happily sending data down a closed/broken
%% socket, but that's ok since a) data in the buffers will be lost in
%% any case (so qualitatively we are no worse off than if we used
%% gen_tcp:send/2), and b) we do detect the changed socket status
%% eventually, i.e. when we get round to handling the result code.
%%
%% Also note that the port has bounded buffers and port_command blocks
%% when these are full. So the fact that we process the result
%% asynchronously does not impact flow control.
internal_send_command_async(MethodRecord,
                            #wstate{sock      = Sock,
                                    channel   = Channel,
                                    protocol  = Protocol}) ->
    true = port_cmd(Sock, assemble_frames(Channel, MethodRecord, Protocol)),
    ok.

internal_send_command_async(MethodRecord, Content,
                            #wstate{sock      = Sock,
                                    channel   = Channel,
                                    frame_max = FrameMax,
                                    protocol  = Protocol}) ->
    true = port_cmd(Sock, assemble_frames(Channel, MethodRecord,
                                          Content, FrameMax, Protocol)),
    ok.

port_cmd(Sock, Data) ->
    try rabbit_net:port_command(Sock, Data)
    catch error:Error -> exit({writer, send_failed, Error})
    end.
