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
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_binary_parser).

-include("rabbit.hrl").

-export([parse_table/1, parse_properties/2]).
-export([ensure_content_decoded/1, clear_decoded_content/1]).

-import(lists).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(parse_table/1 :: (binary()) -> amqp_table()).
-spec(parse_properties/2 :: ([amqp_property_type()], binary()) -> [any()]).
-spec(ensure_content_decoded/1 :: (content()) -> decoded_content()).
-spec(clear_decoded_content/1 :: (content()) -> undecoded_content()).

-endif.

%%----------------------------------------------------------------------------

%% parse_table supports the AMQP 0-8/0-9 standard types, S, I, D, T
%% and F, as well as the QPid extensions b, d, f, l, s, t, x, and V.

parse_table(<<>>) ->
    [];
parse_table(<<NLen:8/unsigned, NameString:NLen/binary, ValueAndRest/binary>>) ->
    {Type, Value, Rest} = parse_field_value(ValueAndRest),
    [{NameString, Type, Value} | parse_table(Rest)].

parse_array(<<>>) ->
    [];
parse_array(<<ValueAndRest/binary>>) ->
    {Type, Value, Rest} = parse_field_value(ValueAndRest),
    [{Type, Value} | parse_array(Rest)].

parse_field_value(<<"S", VLen:32/unsigned, ValueString:VLen/binary, Rest/binary>>) ->
    {longstr, ValueString, Rest};

parse_field_value(<<"I", Value:32/signed, Rest/binary>>) ->
    {signedint, Value, Rest};

parse_field_value(<<"D", Before:8/unsigned, After:32/unsigned, Rest/binary>>) ->
    {decimal, {Before, After}, Rest};

parse_field_value(<<"T", Value:64/unsigned, Rest/binary>>) ->
    {timestamp, Value, Rest};

parse_field_value(<<"F", VLen:32/unsigned, Table:VLen/binary, Rest/binary>>) ->
    {table, parse_table(Table), Rest};

parse_field_value(<<"A", VLen:32/unsigned, Array:VLen/binary, Rest/binary>>) ->
    {array, parse_array(Array), Rest};

parse_field_value(<<"b", Value:8/unsigned, Rest/binary>>) ->
    {byte, Value, Rest};

parse_field_value(<<"d", Value:64/float, Rest/binary>>) ->
    {double, Value, Rest};

parse_field_value(<<"f", Value:32/float, Rest/binary>>) ->
    {float, Value, Rest};

parse_field_value(<<"l", Value:64/signed, Rest/binary>>) ->
    {long, Value, Rest};

parse_field_value(<<"s", Value:16/signed, Rest/binary>>) ->
    {short, Value, Rest};

parse_field_value(<<"t", Value:8/unsigned, Rest/binary>>) ->
    {bool, (Value /= 0), Rest};

parse_field_value(<<"x", VLen:32/unsigned, ValueString:VLen/binary, Rest/binary>>) ->
    {binary, ValueString, Rest};

parse_field_value(<<"V", Rest/binary>>) ->
    {void, undefined, Rest}.


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

parse_properties(_Bit, [], Acc, _FirstShort,
                 _Remainder, <<>>) ->
    lists:reverse(Acc);
parse_properties(_Bit, [], _Acc, _FirstShort,
                 _Remainder, _LeftoverBin) ->
    exit(content_properties_binary_overflow);
parse_properties(15, TypeList, Acc, _OldFirstShort,
                 <<NewFirstShort:16,Remainder/binary>>, Properties) ->
    parse_properties(0, TypeList, Acc, NewFirstShort, Remainder, Properties);
parse_properties(Bit, [Type | TypeListRest], Acc, FirstShort,
                 Remainder, Properties) ->
    {Value, Rest} =
        if (FirstShort band (1 bsl (15 - Bit))) /= 0 ->
                parse_property(Type, Properties);
           Type == bit -> {false    , Properties};
           true        -> {undefined, Properties}
        end,
    parse_properties(Bit + 1, TypeListRest, [Value | Acc], FirstShort,
                     Remainder, Rest).

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

ensure_content_decoded(Content = #content{properties = Props})
  when Props =/= 'none' ->
    Content;
ensure_content_decoded(Content = #content{properties_bin = PropBin})
  when is_binary(PropBin) ->
    Content#content{properties = rabbit_framing:decode_properties(
                                   Content#content.class_id, PropBin)}.

clear_decoded_content(Content = #content{properties = none}) ->
    Content;
clear_decoded_content(Content = #content{properties_bin = none}) ->
    %% Only clear when we can rebuild the properties later in
    %% accordance to the content record definition comment - maximum
    %% one of properties and properties_bin can be 'none'
    Content;
clear_decoded_content(Content = #content{}) ->
    Content#content{properties = none}.
