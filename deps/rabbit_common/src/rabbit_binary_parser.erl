%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_binary_parser).

-include("rabbit.hrl").

-export([parse_table/1]).
-export([ensure_content_decoded/1, clear_decoded_content/1]).
-export([validate_utf8/1, assert_utf8/1]).

%%----------------------------------------------------------------------------

-spec parse_table(binary()) -> rabbit_framing:amqp_table().
-spec ensure_content_decoded
        (rabbit_types:content()) ->
            rabbit_types:decoded_content().
-spec clear_decoded_content
        (rabbit_types:content()) ->
            rabbit_types:undecoded_content().
-spec validate_utf8(binary()) -> 'ok' | 'error'.
-spec assert_utf8(binary()) -> 'ok'.

%%----------------------------------------------------------------------------

%% parse_table supports the AMQP 0-8/0-9 standard types, S, I, D, T
%% and F, as well as the QPid extensions b, d, f, l, s, t, x, and V.

-define(SIMPLE_PARSE_TABLE(BType, Pattern, RType),
        parse_table(<<NLen:8/unsigned, NameString:NLen/binary,
                      BType, Pattern, Rest/binary>>) ->
               [{NameString, RType, Value} | parse_table(Rest)]).

%% Note that we try to put these in approximately the order we expect
%% to hit them, that's why the empty binary is half way through.

parse_table(<<NLen:8/unsigned, NameString:NLen/binary,
              $S, VLen:32/unsigned, Value:VLen/binary, Rest/binary>>) ->
    [{NameString, longstr, Value} | parse_table(Rest)];

?SIMPLE_PARSE_TABLE($T, Value:64/unsigned, timestamp);

parse_table(<<>>) ->
    [];

?SIMPLE_PARSE_TABLE($b, Value:8/signed,  byte);
?SIMPLE_PARSE_TABLE($B, Value:8/unsigned, unsignedbyte);

?SIMPLE_PARSE_TABLE($s, Value:16/signed, short);
?SIMPLE_PARSE_TABLE($u, Value:16/unsigned, unsignedshort);

?SIMPLE_PARSE_TABLE($I, Value:32/signed,   signedint);
?SIMPLE_PARSE_TABLE($i, Value:32/unsigned, unsignedint);

?SIMPLE_PARSE_TABLE($d, Value:64/float, double);
?SIMPLE_PARSE_TABLE($f, Value:32/float, float);

%% yes, both 'l' and 'L' fields are decoded to 64-bit signed values;
%% see https://github.com/rabbitmq/rabbitmq-server/issues/1093#issuecomment-276351183,
%% https://www.rabbitmq.com/amqp-0-9-1-errata.html, and section
%% 4.2.1 of the spec for details.
?SIMPLE_PARSE_TABLE($l, Value:64/signed, long);
?SIMPLE_PARSE_TABLE($L, Value:64/signed, long);


parse_table(<<NLen:8/unsigned, NameString:NLen/binary,
              $t, Value:8/unsigned, Rest/binary>>) ->
    [{NameString, bool, (Value /= 0)} | parse_table(Rest)];

parse_table(<<NLen:8/unsigned, NameString:NLen/binary,
              $D, Before:8/unsigned, After:32/unsigned, Rest/binary>>) ->
    [{NameString, decimal, {Before, After}} | parse_table(Rest)];

parse_table(<<NLen:8/unsigned, NameString:NLen/binary,
              $F, VLen:32/unsigned, Value:VLen/binary, Rest/binary>>) ->
    [{NameString, table, parse_table(Value)} | parse_table(Rest)];

parse_table(<<NLen:8/unsigned, NameString:NLen/binary,
              $A, VLen:32/unsigned, Value:VLen/binary, Rest/binary>>) ->
    [{NameString, array, parse_array(Value)} | parse_table(Rest)];

parse_table(<<NLen:8/unsigned, NameString:NLen/binary,
              $x, VLen:32/unsigned, Value:VLen/binary, Rest/binary>>) ->
    [{NameString, binary, Value} | parse_table(Rest)];

parse_table(<<NLen:8/unsigned, NameString:NLen/binary,
              $V, Rest/binary>>) ->
    [{NameString, void, undefined} | parse_table(Rest)].

-define(SIMPLE_PARSE_ARRAY(BType, Pattern, RType),
        parse_array(<<BType, Pattern, Rest/binary>>) ->
               [{RType, Value} | parse_array(Rest)]).

parse_array(<<$S, VLen:32/unsigned, Value:VLen/binary, Rest/binary>>) ->
    [{longstr, Value} | parse_array(Rest)];

?SIMPLE_PARSE_ARRAY($T, Value:64/unsigned, timestamp);

parse_array(<<>>) ->
    [];

?SIMPLE_PARSE_ARRAY($b, Value:8/signed,  byte);
?SIMPLE_PARSE_ARRAY($B, Value:8/unsigned, unsignedbyte);

?SIMPLE_PARSE_ARRAY($s, Value:16/signed, short);
?SIMPLE_PARSE_ARRAY($u, Value:16/unsigned, unsignedshort);

?SIMPLE_PARSE_ARRAY($I, Value:32/signed,   signedint);
?SIMPLE_PARSE_ARRAY($i, Value:32/unsigned, unsignedint);

?SIMPLE_PARSE_ARRAY($d, Value:64/float, double);
?SIMPLE_PARSE_ARRAY($f, Value:32/float, float);

?SIMPLE_PARSE_ARRAY($l, Value:64/signed, long);
?SIMPLE_PARSE_ARRAY($L, Value:64/signed, long);


parse_array(<<$t, Value:8/unsigned, Rest/binary>>) ->
    [{bool, (Value /= 0)} | parse_array(Rest)];

parse_array(<<$D, Before:8/unsigned, After:32/unsigned, Rest/binary>>) ->
    [{decimal, {Before, After}} | parse_array(Rest)];

parse_array(<<$F, VLen:32/unsigned, Value:VLen/binary, Rest/binary>>) ->
    [{table, parse_table(Value)} | parse_array(Rest)];

parse_array(<<$A, VLen:32/unsigned, Value:VLen/binary, Rest/binary>>) ->
    [{array, parse_array(Value)} | parse_array(Rest)];

parse_array(<<$x, VLen:32/unsigned, Value:VLen/binary, Rest/binary>>) ->
    [{binary, Value} | parse_array(Rest)];

parse_array(<<$V, Rest/binary>>) ->
    [{void, undefined} | parse_array(Rest)].

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

assert_utf8(B) ->
    case validate_utf8(B) of
        ok    -> ok;
        error -> rabbit_misc:protocol_error(
                   frame_error, "Malformed UTF-8 in shortstr", [])
    end.

validate_utf8(Bin) ->
    try
        _ = xmerl_ucs:from_utf8(Bin),
        ok
    catch exit:{ucs, _} ->
            error
    end.
