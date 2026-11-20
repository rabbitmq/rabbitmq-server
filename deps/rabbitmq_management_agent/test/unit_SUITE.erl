%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").

all() ->
    [basic_properties_shortstr_length,
     basic_properties_header_name_length,
     basic_properties_headers_type].

basic_properties_shortstr_length(_Config) ->
    Fields = [content_type, content_encoding, correlation_id, reply_to,
              expiration, message_id, type, user_id, app_id, cluster_id],
    lists:foreach(
      fun(Field) ->
              Key = atom_to_binary(Field, utf8),
              lists:foreach(
                fun(Value) ->
                        Props = rabbit_mgmt_format:to_basic_properties(#{Key => Value}),
                        ?assertEqual([{Field, Value}], rabbit_mgmt_format:basic_properties(Props))
                end, [<<>>, binary:copy(<<"a">>, 255),
                      <<(binary:copy(<<195, 169>>, 127))/binary, "a">>]),
              lists:foreach(
                fun(Value) ->
                        ?assertThrow({error, {string_too_long, Field}},
                                     rabbit_mgmt_format:to_basic_properties(#{Key => Value}))
                end, [binary:copy(<<"a">>, 256), binary:copy(<<195, 169>>, 128)])
      end, Fields),
    ok.

basic_properties_header_name_length(_Config) ->
    lists:foreach(
      fun({Key, Expected}) ->
              Header = #{Key => binary:copy(<<"v">>, 256)},
              lists:foreach(
                fun(Headers) ->
                        Input = #{<<"headers">> => Headers},
                        case Expected of
                            ok ->
                                Props = rabbit_mgmt_format:to_basic_properties(Input),
                                ?assertEqual([{headers, Headers}],
                                             rabbit_mgmt_format:basic_properties(Props));
                            error ->
                                ?assertThrow({error, {string_too_long, headers}},
                                             rabbit_mgmt_format:to_basic_properties(Input))
                        end
                end, [Header, #{<<"nested">> => Header},
                      #{<<"array">> => [42, Header]}, #{<<"array">> => [[Header]]}])
      end, [{<<>>, ok}, {binary:copy(<<"a">>, 255), ok},
            {<<(binary:copy(<<195, 169>>, 127))/binary, "a">>, ok},
            {binary:copy(<<"a">>, 256), error}, {binary:copy(<<195, 169>>, 128), error}]),
    ok.

basic_properties_headers_type(_Config) ->
    lists:foreach(
      fun(Headers) ->
              ?assertThrow({error, {not_table, headers}},
                           rabbit_mgmt_format:to_basic_properties(#{<<"headers">> => Headers}))
      end, [<<"s">>, 5, [<<"x">>], true, null]),
    ?assertEqual(rabbit_mgmt_format:to_basic_properties(#{<<"headers">> => #{}}),
                 rabbit_mgmt_format:to_basic_properties(#{<<"headers">> => []})),
    ok.
