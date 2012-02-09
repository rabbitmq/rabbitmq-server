%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_binary_parser).

-include("rabbit.hrl").

-export([parse_table/1]).
-export([ensure_content_decoded/1, clear_decoded_content/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(parse_table/1 :: (binary()) -> rabbit_framing:amqp_table()).
-spec(ensure_content_decoded/1 ::
        (rabbit_types:content()) -> rabbit_types:decoded_content()).
-spec(clear_decoded_content/1 ::
        (rabbit_types:content()) -> rabbit_types:undecoded_content()).

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

ensure_content_decoded(Content = #content{properties = Props})
  when Props =/= none ->
    Content;
ensure_content_decoded(Content = #content{properties_bin = PropBin,
                                          protocol = Protocol})
  when PropBin =/= none ->
    Content#content{properties = Protocol:decode_properties(
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
