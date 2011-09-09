-module(rabbit_amqp1_0_binary_parser).

-export([parse/1, parse_all/1]).

-include("rabbit_amqp1_0.hrl").

-ifdef(use_specs).
-spec(parse/1 :: (binary()) -> tuple()).
-endif.

parse_all(ValueBin) when is_binary(ValueBin) ->
    lists:reverse(parse_all([], parse(ValueBin))).

parse_all(Acc, {Value, <<>>}) -> [Value | Acc];
parse_all(Acc, {Value, Rest}) -> parse_all([Value | Acc], parse(Rest)).

parse(<<?DESCRIBED,Rest/binary>>) ->
    parse_described(Rest);
parse(Rest) ->
    parse_primitive0(Rest).

parse_described(Bin) ->
    {Descriptor, Rest1} = parse(Bin),
%%    io:format("Descriptor: ~p~n", [Descriptor]),
    {Value, Rest2} = parse(Rest1),
    {{described, Descriptor, Value}, Rest2}.

parse_primitive0(<<Subcategory:4,Subtype:4,Rest/binary>>) ->
    parse_primitive(Subcategory, Subtype, Rest).

parse_primitive(?FIXED_0, 0, Rest) ->
    {null, Rest};
parse_primitive(?FIXED_0, 1, Rest) ->
    {true, Rest};
parse_primitive(?FIXED_0, 2, Rest) ->
    {false, Rest};
parse_primitive(?FIXED_1, 6, <<0:8/unsigned,Rest/binary>>) ->
    {false, Rest};
parse_primitive(?FIXED_1, 6, <<1:8/unsigned,Rest/binary>>) ->
    {true, Rest};


%% Most integral types have a compact encoding as a byte.
%% TODO types other than ulong (ulong is used for descriptors).
parse_primitive(?FIXED_1, 0, <<Value:8/unsigned,Rest/binary>>) ->
    {{ubyte, Value}, Rest};
parse_primitive(?FIXED_2, 0, <<Value:16/unsigned,Rest/binary>>) ->
    {{ushort, Value}, Rest};
parse_primitive(?FIXED_0, 3, Rest) ->
    {{uint, 0}, Rest};
parse_primitive(?FIXED_1, 2, <<Value:8/unsigned,Rest/binary>>) ->
    {{uint, Value}, Rest};
parse_primitive(?FIXED_4, 0, <<Value:32/unsigned,Rest/binary>>) ->
    {{uint, Value}, Rest};
parse_primitive(?FIXED_0, 4, Rest) ->
    {{ulong, 0}, Rest};
parse_primitive(?FIXED_1, 3, <<Value:8/unsigned,Rest/binary>>) ->
    {{ulong, Value}, Rest};
parse_primitive(?FIXED_8, 0, <<Value:64/unsigned,Rest/binary>>) ->
    {{ulong, Value}, Rest};
parse_primitive(?FIXED_1, 1, <<Value:8/signed,Rest/binary>>) ->
    {{byte, Value}, Rest};
parse_primitive(?FIXED_2, 1, <<Value:16/signed,Rest/binary>>) ->
    {{short, Value}, Rest};
parse_primitive(?FIXED_1, 4, <<Value:8/signed,Rest/binary>>) ->
    {{int, Value}, Rest};
parse_primitive(?FIXED_4, 1, <<Value:32/signed,Rest/binary>>) ->
    {{int, Value}, Rest};
parse_primitive(?FIXED_1, 5, <<Value:8/signed,Rest/binary>>) ->
    {{long, Value}, Rest};
parse_primitive(?FIXED_8, 1, <<Value:64/signed,Rest/binary>>) ->
    {{long, Value}, Rest};

parse_primitive(?FIXED_4, 2, <<Value:32/float,Rest/binary>>) ->
    {{float, Value}, Rest};
parse_primitive(?FIXED_8, 2, <<Value:64/float,Rest/binary>>) ->
    {{double, Value}, Rest};

parse_primitive(?FIXED_4, 3, <<Utf32Char:4/binary,Rest/binary>>) ->
    {{char, Utf32Char}, Rest};
parse_primitive(?FIXED_8, 3, <<Timestamp:64/signed,Rest/binary>>) ->
    {{timestamp, Timestamp}, Rest};
parse_primitive(?FIXED_16, 8, <<Uuid:16/binary,Rest/binary>>) ->
    {{uuid, Uuid}, Rest};

parse_primitive(?VAR_1, 0,
                <<Size:8/unsigned, Value:Size/binary,Rest/binary>>) ->
    {{binary, Value}, Rest};
parse_primitive(?VAR_4, 0, 
                <<Size:32/unsigned,Value:Size/binary,Rest/binary>>) ->
    {{binary, Value}, Rest};

parse_primitive(?VAR_1, 1, 
                <<Size:8/unsigned,Value:Size/binary,Rest/binary>>) ->
    {{utf8, Value}, Rest};
parse_primitive(?VAR_1, 2,
                <<Size:8/unsigned,Value:Size/binary,Rest/binary>>) ->
    {{utf16, Value}, Rest};

parse_primitive(?VAR_4, 1,
                <<Size:32/unsigned,Value:Size/binary,Rest/binary>>) ->
    {{utf8, Value}, Rest};
parse_primitive(?VAR_4, 2,
                <<Size:32/unsigned,Value:Size/binary,Rest/binary>>) ->
    {{utf16, Value}, Rest};

parse_primitive(?VAR_1, 3, 
                <<Size:8/unsigned,Value:Size/binary,Rest/binary>>) ->
    {{symbol, binary_to_list(Value)}, Rest};
parse_primitive(?VAR_4, 3,
                <<Size:32/unsigned,Value:Size/binary,Rest/binary>>) ->
    {{symbol, binary_to_list(Value)}, Rest};

parse_primitive(?FIXED_0, 5, Rest) ->
    {{list, []}, Rest};
parse_primitive(?COMPOUND_1, 0,
                <<Size:8/unsigned,
                 CountAndValue:Size/binary,Rest/binary>>) ->
    {{list, parse_compound(8, CountAndValue)}, Rest};
parse_primitive(?COMPOUND_4, 0,
                <<Size:32/unsigned,
                 CountAndValue:Size/binary,Rest/binary>>) ->
    {{list, parse_compound(32, CountAndValue)}, Rest};
parse_primitive(?ARRAY_1, 0,
                <<Size:8/unsigned,
                 CountAndValue:Size/binary,Rest/binary>>) ->
    {{list, parse_array(8, CountAndValue)}, Rest};
parse_primitive(?ARRAY_4, 0,
                <<Size:32/unsigned,
                 CountAndValue:Size/binary,Rest/binary>>) ->
    {{list, parse_array(32, CountAndValue)}, Rest};

parse_primitive(?COMPOUND_1, 1,
               <<Size:8/unsigned,
                CountAndValue:Size/binary,Rest/binary>>) ->
    List = parse_compound(8, CountAndValue),
    {{map, mapify(List)}, Rest};
parse_primitive(?COMPOUND_4, 1,
               <<Size:32/unsigned,
                CountAndValue:Size/binary,Rest/binary>>) ->
    List = parse_compound(32, CountAndValue),
    {{map, mapify(List)}, Rest}.

parse_compound(UnitSize, Bin) ->
    <<Count:UnitSize, Bin1/binary>> = Bin,
    parse_compound1(Count, Bin1, []).

parse_compound1(0, <<>>, List) ->
    lists:reverse(List);
parse_compound1(Count, Bin, Acc) ->
    {Value, Rest} = parse(Bin),
    parse_compound1(Count - 1, Rest, [Value | Acc]).

parse_array(UnitSize, Bin) ->
    <<Count:UnitSize, Bin1/binary>> = Bin,
    parse_array1(Count, Bin1).

parse_array1(Count, <<?DESCRIBED,Rest/binary>>) ->
    {Descriptor, Rest1} = parse(Rest),
    List = parse_array1(Count, Rest1),
    lists:map(fun (Value) ->
                      {described, Descriptor, Value}
              end, List);
parse_array1(Count, <<SubCategory:4,SubType:4,ArrayBin/binary>>) ->
    parse_array2(Count, SubCategory, SubType, ArrayBin, []).

parse_array2(0, _SubCat, _SubType, <<>>, Acc) ->
    lists:reverse(Acc);
parse_array2(Count, SubCat, SubType, Bin, Acc) ->
    {Value, Rest} = parse_primitive(SubCat, SubType, Bin),
    parse_array2(Count - 1, SubCat, SubType, Rest, [Value | Acc]).

mapify([]) ->
    [];
mapify([Key, Value | Rest]) ->
    [{Key, Value} | mapify(Rest)].
