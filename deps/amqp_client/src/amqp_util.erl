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
%%   The Original Code is the RabbitMQ Erlang Client.
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
%%   Contributor(s): Ben Hood <0x6e6562@gmail.com>.
%%

-module(amqp_util).

-include_lib("rabbitmq_server/include/rabbit.hrl").
-include_lib("rabbitmq_server/include/rabbit_framing.hrl").

-export([message_payload/1]).
-export([binary/1]).
-export([basic_properties/0, protocol_header/0]).
-export([decode_method/2]).

basic_properties() ->
    #'P_basic'{content_type = <<"application/octet-stream">>, delivery_mode = 1, priority = 0}.

protocol_header() ->
    <<"AMQP", 1, 1, ?PROTOCOL_VERSION_MAJOR, ?PROTOCOL_VERSION_MINOR>>.

binary(L) when is_list(L) ->
    list_to_binary(L);

binary(B) when is_binary(B) ->
    B.

message_payload(Message) ->
    (Message#basic_message.content)#content.payload_fragments_rev.

decode_method(Method, Content) ->
    case finish_reading_method(Method,Content) of
        {ok, Method2, none}     -> Method2;
        {ok, Method2, Content2} -> {Method2, Content2}
    end.

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

%%--------------------------------------------------------------------

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
        {frame, _Channel, Frame, AckPid} ->
            AckPid ! ack,
            Frame;
        Other -> unexpected_message(Other)
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

unexpected_message(Msg) ->
    rabbit_log:error("Channel received unexpected message: ~p~n", [Msg]),
    exit({unexpected_channel_message, Msg}).

