-module(rabbit_amqp1_0_binary_generator).

-export([generate/1, build_frame/2, build_heartbeat_frame/0]).

-include("rabbit_amqp1_0.hrl").

-ifdef(use_specs).
-spec(generate/1 :: (tuple()) -> iolist()).
-spec(build_frame/2 :: (int(), iolist()) -> iolist()).
-endif.

-define(AMQP_FRAME_TYPE, 0).
-define(DOFF, 2).

build_frame(Channel, Payload) ->
    Size = iolist_size(Payload) + 8, % frame header and no extension
    [ <<Size:32/unsigned, 2:8, ?AMQP_FRAME_TYPE:8, Channel:16/unsigned>>, Payload ].

build_heartbeat_frame() ->
    %% length is inclusive
    <<8:32, ?DOFF:8, ?AMQP_FRAME_TYPE:8, 0:16>>.

generate({described, Descriptor, Value}) ->
    DescBin = generate(Descriptor),
    ValueBin = generate(Value),
    [ ?DESCRIBED_BIN, DescBin, ValueBin ];

generate(null) -> <<?FIXED_0:4,0:4>>;
generate(true) -> <<?FIXED_0:4,1:4>>;
generate(false) -> <<?FIXED_0:4,2:4>>;

%% some integral types have a compact encoding as a byte; this is in
%% particular for the descriptors of AMQP types, which have the domain
%% bits set to zero and values < 256.
generate({ubyte, Value}) -> <<?FIXED_1:4,0:4,Value:8/unsigned>>;
generate({ushort, Value}) -> <<?FIXED_2:4,0:4,Value:16/unsigned>>;
generate({uint, Value}) ->
    if Value < 256 -> <<?FIXED_1:4,2:4,Value:8/unsigned>>;
       true        -> <<?FIXED_4:4,0:4,Value:32/unsigned>>
    end;
generate({ulong, Value}) ->
    if Value < 256 -> <<?FIXED_1:4,3:4,Value:8/unsigned>>;
       true        -> <<?FIXED_8:4,0:4,Value:64/unsigned>>
    end;
generate({byte, Value}) -> <<?FIXED_1:4,1:4,Value:8/signed>>;
generate({short, Value}) -> <<?FIXED_2:4,1:4,Value:16/signed>>;
generate({int, Value}) ->
    if Value < 128 andalso
       Value > -129 -> <<?FIXED_1:4,4:4,Value:8/signed>>;
       true         -> <<?FIXED_4:4,1:4,Value:32/signed>>
    end;
generate({long, Value}) ->
    if Value < 128 andalso
       Value > -129 -> <<?FIXED_1:4,5:4,Value:8/signed>>;
       true         -> <<?FIXED_8:4,1:4,Value:64/signed>>
    end;

generate({float, Value}) -> <<?FIXED_4:4,2:4,Value:32/float>>;
generate({double, Value}) -> <<?FIXED_8:4,2:4,Value:64/float>>;

generate({char, Value}) -> <<?FIXED_4:4,3:4,Value:4/binary>>;
generate({timestamp, Value}) -> <<?FIXED_8:4,3:4,Value:64/signed>>;
generate({uuid, Value}) -> <<?FIXED_16:4,8:4,Value:16/binary>>;

generate({binary, Value}) ->
    Size = iolist_size(Value),
    if  Size < 16#ff ->
            [ <<?VAR_1:4,0:4,Size:8>>, Value ];
        true ->
            [ <<?VAR_4:4,0:4,Size:32>>, Value ]
    end;

%% FIXME variable sizes too
generate({utf8, Value}) -> [ <<?VAR_1:4,1:4,(size(Value)):8>>, Value ];
generate({utf16, Value}) -> [ <<?VAR_1:4,2:4,(size(Value)):8>>, Value ];

generate({symbol, Value}) -> [ <<?VAR_1:4,3:4,(length(Value)):8>>,
                               list_to_binary(Value) ];

generate({list, []}) ->
    <<?FIXED_0:4, 5:4>>;
generate({list, List}) ->
    Count = length(List),
    Compound = lists:map(fun generate/1, List),
    Size = iolist_size(Compound),
    if Size > 255  -> % Size < 256 -> Count < 256
            [ <<?COMPOUND_4:4, 0:4, (Size + 4):32/unsigned, Count:32/unsigned>>,
              Compound ];
       true ->
            [ <<?COMPOUND_1:4, 0:4, (Size + 1):8/unsigned, Count:8/unsigned>>,
              Compound ]
    end;

generate({map, ListOfPairs}) ->
    Count = length(ListOfPairs) * 2,
    Compound = lists:map(fun ({Key, Val}) ->
                                 [(generate(Key)),
                                  (generate(Val))]
                         end, ListOfPairs),
    Size = iolist_size(Compound),
    if Size > 255 ->
            [ <<?COMPOUND_4:4,1:4,(Size + 4):32,Count:32>>,
              Compound ];
       true ->
            [ <<?COMPOUND_1:4,1:4,(Size + 1):8,Count:8>>,
              Compound ]
    end;

generate({array, Type, List}) ->
    Count = length(List),
    Body = iolist_to_binary(
             [constructor(Type), [generate(Type, I) || I <- List]]),
    Size = size(Body),
    if Size > 255  -> % Size < 256 -> Count < 256
            [ <<?ARRAY_4:4, 0:4, (Size + 4):32/unsigned, Count:32/unsigned>>,
              Body ];
       true ->
            [ <<?ARRAY_1:4, 0:4, (Size + 1):8/unsigned, Count:8/unsigned>>,
              Body ]
    end.

%% TODO again these are a stub to get SASL working. New codec? Will
%% that ever happen? If not we really just need to split generate/1
%% up into things like these...
constructor(symbol) ->
     <<?VAR_1:4,3:4>>.

generate(symbol, Value) ->
    [<<(length(Value)):8>>, list_to_binary(Value)].
