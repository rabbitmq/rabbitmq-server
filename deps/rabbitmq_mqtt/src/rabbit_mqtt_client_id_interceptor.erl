-module(rabbit_mqtt_client_id_interceptor).

-behaviour(rabbit_incoming_message_interceptor).

-define(ANN_CLIENT_ID, <<"client_id">>).

-export([
    intercept/4
]).

intercept(Msg, ProtoAccessor, ProtoState, _Config) ->
    case rabbit_protocol_accessor:get_proto_state_properties(ProtoAccessor, ProtoState, [client_id]) of
        [{client_id, ClientId}] ->
            rabbit_incoming_message_interceptor:set_msg_annotation(
                Msg,
                ?ANN_CLIENT_ID,
                ClientId,
                true);
        [] ->
            Msg
    end.
