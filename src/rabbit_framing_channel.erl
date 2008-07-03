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
%%   Portions created by LShift Ltd., Cohesive Financial Technologies
%%   LLC., and Rabbit Technologies Ltd. are Copyright (C) 2007-2008
%%   LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_framing_channel).
-include("rabbit.hrl").

-export([start_link/2, process/2, shutdown/1]).

%% internal
-export([mainloop/1]).

%%--------------------------------------------------------------------

start_link(StartFun, StartArgs) ->
    spawn_link(
      fun () ->
              %% we trap exits so that a normal termination of the
              %% channel or reader process terminates us too.
              process_flag(trap_exit, true),
              mainloop(apply(StartFun, StartArgs))
      end).

process(Pid, Frame) ->
    Pid ! {frame, Frame},
    ok.

shutdown(Pid) ->
    Pid ! terminate,
    ok.

%%--------------------------------------------------------------------

read_frame(ChannelPid) ->
    receive
        %% converting the exit signal into one of our own ensures that
        %% the reader sees the right pid (i.e. ours) when a channel
        %% exits. Similarly in the other direction, though it is not
        %% really relevant there since the channel is not specifically
        %% watching out for reader exit signals.
        {'EXIT', _Pid, Reason} -> exit(Reason);
        {frame, Frame}         -> Frame;
        terminate              -> rabbit_channel:shutdown(ChannelPid),
                                  read_frame(ChannelPid);
        Msg                    -> exit({unexpected_message, Msg})
    end.

mainloop(ChannelPid) ->
    {method, MethodName, FieldsBin} = read_frame(ChannelPid),
    Method = rabbit_framing:decode_method_fields(MethodName, FieldsBin),
    case rabbit_framing:method_has_content(MethodName) of
        true  -> rabbit_channel:do(ChannelPid, Method,
                                   collect_content(ChannelPid, MethodName));
        false -> rabbit_channel:do(ChannelPid, Method)
    end,
    ?MODULE:mainloop(ChannelPid).

collect_content(ChannelPid, MethodName) ->
    {ClassId, _MethodId} = rabbit_framing:method_id(MethodName),
    case read_frame(ChannelPid) of
        {content_header, HeaderClassId, 0, BodySize, PropertiesBin} ->
            if HeaderClassId == ClassId ->
                    Payload = collect_content_payload(ChannelPid, BodySize, []),
                    #content{class_id = ClassId,
                             properties = none,
                             properties_bin = PropertiesBin,
                             payload_fragments_rev = Payload};
               true ->
                    rabbit_misc:protocol_error(
                      command_invalid,
                      "expected content header for class ~w, got one for class ~w instead",
                      [ClassId, HeaderClassId])
            end;
        _ ->
            rabbit_misc:protocol_error(
              command_invalid,
              "expected content header for class ~w, got non content header frame instead",
              [ClassId])
    end.

collect_content_payload(_ChannelPid, 0, Acc) ->
    Acc;
collect_content_payload(ChannelPid, RemainingByteCount, Acc) ->
    case read_frame(ChannelPid) of
        {content_body, FragmentBin} ->
            collect_content_payload(ChannelPid,
                                    RemainingByteCount - size(FragmentBin),
                                    [FragmentBin | Acc]);
        _ -> 
            rabbit_misc:protocol_error(
              command_invalid,
              "expected content body, got non content body frame instead",
              [])
    end.
