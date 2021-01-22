%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_command_assembler).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([analyze_frame/3, init/1, process/2]).

%%----------------------------------------------------------------------------

%%----------------------------------------------------------------------------

-export_type([frame/0]).

-type frame_type() :: ?FRAME_METHOD | ?FRAME_HEADER | ?FRAME_BODY |
                      ?FRAME_OOB_METHOD | ?FRAME_OOB_HEADER | ?FRAME_OOB_BODY |
                      ?FRAME_TRACE | ?FRAME_HEARTBEAT.
-type protocol()   :: rabbit_framing:protocol().
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
        {'method',         protocol()} |
        {'content_header', method(), class_id(), protocol()} |
        {'content_body',   method(), body_size(), class_id(), protocol()}.

-spec analyze_frame(frame_type(), binary(), protocol()) ->
          frame() | 'heartbeat' | 'error'.

-spec init(protocol()) -> {ok, state()}.
-spec process(frame(), state()) ->
          {ok, state()} |
          {ok, method(), state()} |
          {ok, method(), content(), state()} |
          {error, rabbit_types:amqp_error()}.

%%--------------------------------------------------------------------

analyze_frame(?FRAME_METHOD,
              <<ClassId:16, MethodId:16, MethodFields/binary>>,
              Protocol) ->
    MethodName = Protocol:lookup_method_name({ClassId, MethodId}),
    {method, MethodName, MethodFields};
analyze_frame(?FRAME_HEADER,
              <<ClassId:16, Weight:16, BodySize:64, Properties/binary>>,
              _Protocol) ->
    {content_header, ClassId, Weight, BodySize, Properties};
analyze_frame(?FRAME_BODY, Body, _Protocol) ->
    {content_body, Body};
analyze_frame(?FRAME_HEARTBEAT, <<>>, _Protocol) ->
    heartbeat;
analyze_frame(_Type, _Body, _Protocol) ->
    error.

init(Protocol) -> {ok, {method, Protocol}}.

process({method, MethodName, FieldsBin}, {method, Protocol}) ->
    try
        Method = Protocol:decode_method_fields(MethodName, FieldsBin),
        case Protocol:method_has_content(MethodName) of
            true  -> {ClassId, _MethodId} = Protocol:method_id(MethodName),
                     {ok, {content_header, Method, ClassId, Protocol}};
            false -> {ok, Method, {method, Protocol}}
        end
    catch exit:#amqp_error{} = Reason -> {error, Reason}
    end;
process(_Frame, {method, _Protocol}) ->
    unexpected_frame("expected method frame, "
                     "got non method frame instead", [], none);
process({content_header, ClassId, 0, 0, PropertiesBin},
        {content_header, Method, ClassId, Protocol}) ->
    Content = empty_content(ClassId, PropertiesBin, Protocol),
    {ok, Method, Content, {method, Protocol}};
process({content_header, ClassId, 0, BodySize, PropertiesBin},
        {content_header, Method, ClassId, Protocol}) ->
    Content = empty_content(ClassId, PropertiesBin, Protocol),
    {ok, {content_body, Method, BodySize, Content, Protocol}};
process({content_header, HeaderClassId, 0, _BodySize, _PropertiesBin},
        {content_header, Method, ClassId, _Protocol}) ->
    unexpected_frame("expected content header for class ~w, "
                     "got one for class ~w instead",
                     [ClassId, HeaderClassId], Method);
process(_Frame, {content_header, Method, ClassId, _Protocol}) ->
    unexpected_frame("expected content header for class ~w, "
                     "got non content header frame instead", [ClassId], Method);
process({content_body, FragmentBin},
        {content_body, Method, RemainingSize,
         Content = #content{payload_fragments_rev = Fragments}, Protocol}) ->
    NewContent = Content#content{
                   payload_fragments_rev = [FragmentBin | Fragments]},
    case RemainingSize - size(FragmentBin) of
        0  -> {ok, Method, NewContent, {method, Protocol}};
        Sz -> {ok, {content_body, Method, Sz, NewContent, Protocol}}
    end;
process(_Frame, {content_body, Method, _RemainingSize, _Content, _Protocol}) ->
    unexpected_frame("expected content body, "
                     "got non content body frame instead", [], Method).

%%--------------------------------------------------------------------

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
