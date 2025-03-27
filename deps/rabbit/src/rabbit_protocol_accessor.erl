-module(rabbit_protocol_accessor).

-callback get_property(Property, State) -> Resp when
    Property :: atom(),
    State :: term(),
    Resp :: term().

-export([
    get_proto_state_properties/3
]).

-spec get_proto_state_properties(
    ProtoAccessor :: module(),
    ProtoState :: term(),
    Properties :: [Property]
) -> Resp :: [{Property, term()}] when
    Property :: atom().
get_proto_state_properties(ProtoAccessor, ProtoState, Properties) ->
    lists:foldl(
        fun(Property, Results) ->
            case ProtoAccessor:get_property(Property, ProtoState) of
                undefined -> Results;
                Value -> [{Property, Value} | Results]
            end
        end,
        [],
        Properties).
