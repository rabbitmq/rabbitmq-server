%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_command_assembler).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([analyze_frame/2, init/0, process/2]).

%%----------------------------------------------------------------------------

%%----------------------------------------------------------------------------

-export_type([frame/0]).

-type frame_type() :: ?FRAME_METHOD | ?FRAME_HEADER | ?FRAME_BODY |
                      ?FRAME_HEARTBEAT.
-type method()     :: rabbit_framing:amqp_method_record().
-type class_id()   :: rabbit_framing:amqp_class_id().
-type weight()     :: non_neg_integer().
-type body_size()  :: non_neg_integer().
-type content()    :: rabbit_types:undecoded_content().

-type frame() ::
        {'method',         rabbit_framing:amqp_method_name(), binary()} |
        {'content_header', class_id(), weight(), body_size(), binary()} |
        {'content_body',   binary()}.

-type state() ::
        'method' |
        {'content_header', method(), class_id()} |
        {'content_body',   method(), body_size(), class_id()}.

-spec analyze_frame(frame_type(), binary()) ->
          frame() | 'heartbeat' | 'error'.

-spec init() -> {ok, state()}.
-spec process(frame(), state()) ->
          {ok, state()} |
          {ok, method(), state()} |
          {ok, method(), content(), state()} |
          {error, rabbit_types:amqp_error()}.

%%--------------------------------------------------------------------

analyze_frame(?FRAME_METHOD,
              <<ClassId:16, MethodId:16, MethodFields/binary>>) ->
    MethodName = rabbit_framing_amqp_0_9_1:lookup_method_name({ClassId, MethodId}),
    {method, MethodName, MethodFields};
analyze_frame(?FRAME_HEADER,
              <<ClassId:16, Weight:16, BodySize:64, Properties/binary>>) ->
    {content_header, ClassId, Weight, BodySize, Properties};
analyze_frame(?FRAME_BODY, Body) ->
    {content_body, Body};
analyze_frame(?FRAME_HEARTBEAT, <<>>) ->
    heartbeat;
analyze_frame(_Type, _Body) ->
    error.

init() -> {ok, method}.

process({method, MethodName, FieldsBin}, method) ->
    try
        Method = rabbit_framing_amqp_0_9_1:decode_method_fields(MethodName, FieldsBin),
        case rabbit_framing_amqp_0_9_1:method_has_content(MethodName) of
            true  -> {ClassId, _MethodId} = rabbit_framing_amqp_0_9_1:method_id(MethodName),
                     {ok, {content_header, Method, ClassId}};
            false -> {ok, Method, method}
        end
    catch exit:#amqp_error{} = Reason -> {error, Reason}
    end;
process(_Frame, method) ->
    unexpected_frame("expected method frame, "
                     "got non method frame instead", [], none);
process({content_header, ClassId, 0, 0, PropertiesBin},
        {content_header, Method, ClassId}) ->
    Content = empty_content(ClassId, PropertiesBin),
    {ok, Method, Content, method};
process({content_header, ClassId, 0, BodySize, PropertiesBin},
        {content_header, Method, ClassId}) ->
    Content = empty_content(ClassId, PropertiesBin),
    {ok, {content_body, Method, BodySize, Content}};
process({content_header, HeaderClassId, 0, _BodySize, _PropertiesBin},
        {content_header, Method, ClassId}) ->
    unexpected_frame("expected content header for class ~w, "
                     "got one for class ~w instead",
                     [ClassId, HeaderClassId], Method);
process(_Frame, {content_header, Method, ClassId}) ->
    unexpected_frame("expected content header for class ~w, "
                     "got non content header frame instead", [ClassId], Method);
process({content_body, FragmentBin},
        {content_body, Method, RemainingSize,
         Content = #content{payload_fragments_rev = Fragments}}) ->
    NewContent = Content#content{
                   payload_fragments_rev = [FragmentBin | Fragments]},
    case RemainingSize - size(FragmentBin) of
        0  -> {ok, Method, NewContent, method};
        Sz -> {ok, {content_body, Method, Sz, NewContent}}
    end;
process(_Frame, {content_body, Method, _RemainingSize, _Content}) ->
    unexpected_frame("expected content body, "
                     "got non content body frame instead", [], Method).

%%--------------------------------------------------------------------

empty_content(ClassId, PropertiesBin) ->
    #content{class_id              = ClassId,
             properties            = none,
             properties_bin        = PropertiesBin,
             protocol              = rabbit_framing_amqp_0_9_1,
             payload_fragments_rev = []}.

unexpected_frame(Format, Params, Method) when is_atom(Method) ->
    {error, rabbit_misc:amqp_error(unexpected_frame, Format, Params, Method)};
unexpected_frame(Format, Params, Method) ->
    unexpected_frame(Format, Params, rabbit_misc:method_record_type(Method)).
