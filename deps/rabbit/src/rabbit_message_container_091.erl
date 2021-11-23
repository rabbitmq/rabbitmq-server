-module(rabbit_message_container_091).

-include_lib("rabbit_common/include/rabbit.hrl").
-export([get_internal/2, prepare_to_store/1, serialize/1, msg_size/1,
         set_internal/3]).

get_internal(#basic_message{routing_keys = RKs}, routing_keys) ->
    RKs;
get_internal(#basic_message{exchange_name = XName}, exchange_name) ->
    XName;
get_internal(#basic_message{content = Content}, content) ->
    Content;
get_internal(#basic_message{id = Id}, id) ->
    Id;
get_internal(#basic_message{is_persistent = IsPersistent}, is_persistent) ->
    IsPersistent.

set_internal(Msg, routing_keys, Value) ->
    Msg#basic_message{routing_keys = Value};
set_internal(Msg, exchange_name, Value) ->
    Msg#basic_message{exchange_name = Value};
set_internal(Msg, id, Value) ->
    Msg#basic_message{id = Value};
set_internal(Msg, content, Value) ->
    Msg#basic_message{content = Value}.

%% TODO This would make sense to store any type of message on disk.
%% It should clear any unnecesary data
prepare_to_store(Msg) ->
    Msg#basic_message{
      %% don't persist any recoverable decoded properties
      content = rabbit_binary_parser:clear_decoded_content(
                  Msg#basic_message.content)}.

serialize(Msg) ->
    term_to_binary(Msg).

msg_size(Msg) ->
    rabbit_basic:msg_size(Msg).
