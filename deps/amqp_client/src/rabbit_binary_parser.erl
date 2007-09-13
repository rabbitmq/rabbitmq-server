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
%%   The Original Code is RabbitMQ.
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
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_binary_parser).

-include("rabbit.hrl").

-export([parse_binary/2, parse_table/1, parse_properties/2]).
-export([ensure_content_decoded/1]).

-import(lists).

% This is just lovely. This is how binary parsing should
% always be done: with pattern matching and unification.
% Hooray for Erlang! ;-)
parse_binary(List, Binary) ->
    case catch parse_binary_h(List, Binary, []) of
        {'EXIT', _Reason} -> syntax_error;
        Result = {_Values, _Rest} -> Result
    end.

parse_binary_h([], Rest, Acc) ->
    {lists:reverse(Acc), Rest};

parse_binary_h([shortstr|Tail], <<Len:8/unsigned, String:Len/binary, Rest/binary>>, Acc) ->
    parse_binary_h(Tail, Rest, [String | Acc]);

parse_binary_h([longstr|Tail], <<Len:32/unsigned, String:Len/binary, Rest/binary>>, Acc) ->
    parse_binary_h(Tail, Rest, [String | Acc]);

parse_binary_h([octet|Tail], <<Int:8/unsigned, Rest/binary>>, Acc) ->
    parse_binary_h(Tail, Rest, [Int | Acc]);

parse_binary_h([shortint|Tail], <<Int:16/unsigned, Rest/binary>>, Acc) ->
    parse_binary_h(Tail, Rest, [Int | Acc]);

parse_binary_h([longint|Tail], <<Int:32/unsigned, Rest/binary>>, Acc) ->
    parse_binary_h(Tail, Rest, [Int | Acc]);

parse_binary_h([longlongint|Tail], <<Int:64/unsigned, Rest/binary>>, Acc) ->
    parse_binary_h(Tail, Rest, [Int | Acc]);

parse_binary_h([timestamp|Tail], <<Int:64/unsigned, Rest/binary>>, Acc) ->
    parse_binary_h(Tail, Rest, [Int | Acc]);

parse_binary_h([bit|_Tail]=Spec, Rest, Acc) ->
    parse_bit(Spec, 0, 0, Rest, Acc);

parse_binary_h([table|Tail], <<Len:32/unsigned, Table:Len/binary, Rest/binary>>, Acc) ->
    parse_binary_h(Tail, Rest, [parse_table(Table) | Acc]).


parse_bit([bit|_Tail]=Spec, 0, _Cache, <<Bits:8, Rest/binary>>, Acc) ->
    parse_bit(Spec, 8, Bits, Rest, Acc);

parse_bit([bit|Tail], Remaining, Cache, Rest, Acc) ->
    Bitval = case (Cache band 1) of
                 1 -> true;
                 0 -> false
             end,
    parse_bit(Tail, Remaining - 1, Cache bsr 1, Rest, [Bitval | Acc]);

parse_bit(Tail, _Remaining, _Cache, Rest, Acc) ->
    parse_binary_h(Tail, Rest, Acc).


parse_table(<<>>) ->
    [];

parse_table(<<NLen:8/unsigned, NameString:NLen/binary, "S", VLen:32/unsigned, ValueString:VLen/binary, Rest/binary>>) ->
    [{NameString, longstr, ValueString} | parse_table(Rest)];

parse_table(<<NLen:8/unsigned, NameString:NLen/binary, "I", Value:32/signed, Rest/binary>>) ->
    [{NameString, signedint, Value} | parse_table(Rest)];

parse_table(<<NLen:8/unsigned, NameString:NLen/binary, "D", Before:8/unsigned, After:32/unsigned, Rest/binary>>) ->
    [{NameString, decimal, {Before, After}} | parse_table(Rest)];

parse_table(<<NLen:8/unsigned, NameString:NLen/binary, "T", Value:64/unsigned, Rest/binary>>) ->
    [{NameString, timestamp, Value} | parse_table(Rest)];

parse_table(<<NLen:8/unsigned, NameString:NLen/binary, "F", VLen:32/unsigned, Table:VLen/binary, Rest/binary>>) ->
    [{NameString, table, parse_table(Table)} | parse_table(Rest)].


parse_properties([], _PropBin) ->
    [];
parse_properties(TypeList, PropBin) ->
    FlagCount = length(TypeList),
    %% round up to the nearest multiple of 15 bits, since the 16th bit
    %% in each short is a "continuation" bit.
    FlagsLengthBytes = trunc((FlagCount + 14) / 15) * 2,
    <<Flags:FlagsLengthBytes/binary, Properties/binary>> = PropBin,
    <<FirstShort:16, Remainder/binary>> = Flags,
    parse_properties(0, TypeList, [], FirstShort, Remainder, Properties).

parse_properties(_Bit, [], Acc, _FirstShort, _Remainder, <<>>) ->
    lists:reverse(Acc);
parse_properties(_Bit, [], _Acc, _FirstShort, _Remainder, _LeftoverBin) ->
    exit(content_properties_binary_overflow);
parse_properties(15, TypeList, Acc, _OldFirstShort, <<NewFirstShort:16,Remainder/binary>>, Properties) ->
    parse_properties(0, TypeList, Acc, NewFirstShort, Remainder, Properties);
parse_properties(Bit, [Type | TypeListRest], Acc, FirstShort, Remainder, Properties) ->
    if
        (FirstShort band (1 bsl (15 - Bit))) /= 0 ->
            {Value, Rest} = parse_property(Type, Properties),
            parse_properties(Bit + 1, TypeListRest, [Value | Acc], FirstShort, Remainder, Rest);
        Type == bit ->
            parse_properties(Bit + 1, TypeListRest, [false | Acc], FirstShort, Remainder, Properties);
        true ->
            parse_properties(Bit + 1, TypeListRest, [undefined | Acc], FirstShort, Remainder, Properties)
    end.
 
parse_property(shortstr, <<Len:8/unsigned, String:Len/binary, Rest/binary>>) ->
    {String, Rest};
parse_property(longstr, <<Len:32/unsigned, String:Len/binary, Rest/binary>>) ->
    {String, Rest};
parse_property(octet, <<Int:8/unsigned, Rest/binary>>) ->
    {Int, Rest};
parse_property(shortint, <<Int:16/unsigned, Rest/binary>>) ->
    {Int, Rest};
parse_property(longint, <<Int:32/unsigned, Rest/binary>>) ->
    {Int, Rest};
parse_property(longlongint, <<Int:64/unsigned, Rest/binary>>) ->
    {Int, Rest};
parse_property(timestamp, <<Int:64/unsigned, Rest/binary>>) ->
    {Int, Rest};
parse_property(bit, Rest) ->
    {true, Rest};
parse_property(table, <<Len:32/unsigned, Table:Len/binary, Rest/binary>>) ->
    {parse_table(Table), Rest}.


ensure_content_decoded(Content = #content{properties = none, properties_bin = PropBin})
  when is_binary(PropBin) ->
    DecodedProperties = rabbit_framing:decode_properties(Content#content.class_id, PropBin),
    Content#content{properties = DecodedProperties};
ensure_content_decoded(Content = #content{}) ->
    Content.
