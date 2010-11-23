-module(rabbit_amqp1_0_binary_generator).

-export([generate/1, build_frame/2]).

-include("rabbit_amqp1_0.hrl").

-ifdef(use_specs).
-spec(generate/1 :: (tuple()) -> iolist()).
-spec(build_frame/2 :: (int(), iolist()) -> iolist()).
-endif.

build_frame(Channel, Payload) ->
    Size = iolist_size(Payload) + 8, % frame header and no extension
    [ <<Size:32/unsigned, 2:8, 0:8, Channel:16/unsigned>>, Payload ].

generate({described, Descriptor, Value}) ->
    DescBin = generate(Descriptor),
    ValueBin = generate(Value),
    [ ?DESCRIBED_BIN, DescBin, ValueBin ];

generate(null) -> <<?FIXED_0:4,0:4>>;
generate(true) -> <<?FIXED_0:4,1:4>>;
generate(false) -> <<?FIXED_0:4,2:4>>;

generate({ubyte, Value}) -> <<?FIXED_1:4,0:4,Value:8/unsigned>>;
generate({ushort, Value}) -> <<?FIXED_2:4,0:4,Value:16/unsigned>>;
generate({uint, Value}) -> <<?FIXED_4:4,0:4,Value:32/unsigned>>;
generate({ulong, Value}) -> <<?FIXED_8:4,0:4,Value:64/unsigned>>;
generate({byte, Value}) -> <<?FIXED_1:4,1:4,Value:8/signed>>;
generate({short, Value}) -> <<?FIXED_2:4,1:4,Value:16/signed>>;
generate({int, Value}) -> <<?FIXED_4:4,1:4,Value:32/signed>>;
generate({long, Value}) -> <<?FIXED_8:4,1:4,Value:64/signed>>;

generate({float, Value}) -> <<?FIXED_4:4,2:4,Value:32/float>>;
generate({double, Value}) -> <<?FIXED_8:4,2:4,Value:64/float>>;

generate({char, Value}) -> <<?FIXED_4:4,3:4,Value:4/binary>>;
generate({timestamp, Value}) -> <<?FIXED_8:4,3:4,Value:64/signed>>;
generate({uuid, Value}) -> <<?FIXED_16:4,8:4,Value:16/binary>>;

generate({binary, Value}) -> [ <<?VAR_1:4,0:4,(size(Value)):8>>, Value ];
generate({utf8, Value}) -> [ <<?VAR_1:4,1:4,(size(Value)):8>>, Value ];
generate({utf16, Value}) -> [ <<?VAR_1:4,2:4,(size(Value)):8>>, Value ];

generate({symbol, Value}) -> [ <<?VAR_1:4,3:4,(length(Value)):8>>,
                              list_to_binary(Value) ];

generate({list, List}) ->
    Count = length(List),
    Compound = lists:map(fun generate/1, List),
    Size = iolist_size(Compound),
    %% TODO look at the size to see if it needs a different encoding
    [ <<?COMPOUND_1:4, 0:4, (Size + 1):8/unsigned, Count:8/unsigned>>,
      Compound ];

generate({map, ListOfPairs}) ->
    Count = length(ListOfPairs) * 2,
    Compound = lists:map(fun ({Key, Val}) ->
                                 [(generate(Key))/binary,
                                  (generate(Val))/binary]
                         end, ListOfPairs),
    Size = iolist_size(Compound),
    [ <<?COMPOUND_1:4,1:4,(Size + 1):8,Count:8>>,
       Compound ].
