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

-module(rabbit_framing_channel).
-include("rabbit.hrl").

-export([start_link/3, process/2, shutdown/1]).

%% internal
-export([mainloop/3]).

%%--------------------------------------------------------------------

start_link(Parent, ChannelPid, Protocol) ->
    {ok, proc_lib:spawn_link(
           fun () -> mainloop(Parent, ChannelPid, {method, Protocol}) end)}.

process(Pid, Frame) ->
    Pid ! {frame, Frame},
    ok.

shutdown(Pid) ->
    Pid ! terminate,
    ok.

%%--------------------------------------------------------------------

mainloop(Parent, ChannelPid, State) ->
    Loop = fun (NewState) ->
                   ?MODULE:mainloop(Parent, ChannelPid, NewState)
           end,
    receive
        {frame, Frame} ->
            case collect(Frame, State) of
                {ok, NewState} ->
                    Loop(NewState);
                {ok, Method, NewState} ->
                    rabbit_channel:do(ChannelPid, Method),
                    Loop(NewState);
                {ok, Method, Content, NewState} ->
                    rabbit_channel:do(ChannelPid, Method, Content),
                    Loop(NewState);
                {error, Reason} ->
                    Parent ! {channel_exit, self(), Reason}
            end;
        terminate ->
            rabbit_channel:shutdown(ChannelPid),
            Loop(State);
        Msg ->
            exit({unexpected_message, Msg})
    end.

collect({method, MethodName, FieldsBin}, {method, Protocol}) ->
    Method = Protocol:decode_method_fields(MethodName, FieldsBin),
    case Protocol:method_has_content(MethodName) of
        true  -> {ClassId, _MethodId} = Protocol:method_id(MethodName),
                 {ok, {content_header, Method, ClassId, Protocol}};
        false -> {ok, Method, {method, Protocol}}
    end;
collect(_Frame, {method, _Protocol}) ->
    unexpected_frame("expected method frame, "
                     "got non method frame instead", [], none);
collect({content_header, ClassId, 0, 0, PropertiesBin},
        {content_header, Method, ClassId, Protocol}) ->
    Content = empty_content(ClassId, PropertiesBin, Protocol),
    {ok, Method, Content, {method, Protocol}};
collect({content_header, ClassId, 0, BodySize, PropertiesBin},
        {content_header, Method, ClassId, Protocol}) ->
    Content = empty_content(ClassId, PropertiesBin, Protocol),
    {ok, {content_body, Method, BodySize, Content, Protocol}};
collect({content_header, HeaderClassId, 0, _BodySize, _PropertiesBin},
        {content_header, Method, ClassId, _Protocol}) ->
    unexpected_frame("expected content header for class ~w, "
                     "got one for class ~w instead",
                     [ClassId, HeaderClassId], Method);
collect(_Frame, {content_header, Method, ClassId, _Protocol}) ->
    unexpected_frame("expected content header for class ~w, "
                     "got non content header frame instead", [ClassId], Method);
collect({content_body, FragmentBin},
        {content_body, Method, RemainingSize,
         Content = #content{payload_fragments_rev = Fragments}, Protocol}) ->
    NewContent = Content#content{
                   payload_fragments_rev = [FragmentBin | Fragments]},
    case RemainingSize - size(FragmentBin) of
        0  -> {ok, Method, NewContent, {method, Protocol}};
        Sz -> {ok, {content_body, Method, Sz, NewContent, Protocol}}
    end;
collect(_Frame, {content_body, Method, _RemainingSize, _Content, _Protocol}) ->
    unexpected_frame("expected content body, "
                     "got non content body frame instead", [], Method).

empty_content(ClassId, PropertiesBin, Protocol) ->
    #content{class_id              = ClassId,
             properties            = none,
             properties_bin        = PropertiesBin,
             protocol              = Protocol,
             payload_fragments_rev = []}.

unexpected_frame(Format, Params, Method) when is_atom(Method) ->
    {error, rabbit_misc:amqp_error(unexpected_frame, Format, Params, Method)};
unexpected_frame(Format, Params, Method) ->
    unexpected_frame(Format, Params, rabbit_misc:method_record_type(Method)).
