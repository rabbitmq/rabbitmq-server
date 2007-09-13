-module(amqp_util).

-include("rabbit.hrl").
-include("rabbit_framing.hrl").

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
    case rabbit_framing:method_has_content(Method) of
        true ->
            Decoded = #content{class_id = ClassId,
                               properties = Properties,
                               properties_bin = PropertiesBin,
                               payload_fragments_rev = Payload }
        = rabbit_framing_channel:collect_content(Method),
                    CollectedContent =
                            Decoded#content{properties_bin
        = rabbit_framing:decode_properties(ClassId, PropertiesBin)},
                    DecodedMethod = rabbit_framing:decode_method_fields(Method, Content),
                    {DecodedMethod, CollectedContent};
        _ ->
            rabbit_framing:decode_method_fields(Method, Content)
        end.



