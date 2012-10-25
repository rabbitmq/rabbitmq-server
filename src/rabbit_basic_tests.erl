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
-module(rabbit_basic_tests).

-include("rabbit.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(XDEATH_TABLE,
        [{<<"reason">>,       longstr,   <<"blah">>},
         {<<"queue">>,        longstr,   <<"foo.bar.baz">>},
         {<<"exchange">>,     longstr,   <<"my-exchange">>},
         {<<"routing-keys">>, array,     []}]).

-define(ROUTE_TABLE, [{<<"redelivered">>, bool, <<"true">>}]).

write_table_with_invalid_existing_type_test_() ->
    [{"existing entries with invalid types are moved to a table "
      "stored as <<\"x-invalid-headers header\">>",
      fun() ->
              check_invalid(<<"x-death">>,
                            {longstr, <<"this should be a table!!!">>},
                            ?XDEATH_TABLE)
      end},
     {"if invalid existing headers are moved, newly added "
      "ones are still stored correctly",
      begin
          BadHeaders = [{<<"x-received-from">>,
                         longstr, <<"this should be a table!!!">>}],
          Headers = rabbit_basic:prepend_table_header(
                      <<"x-received-from">>, ?ROUTE_TABLE, BadHeaders),
          ?_assertEqual({array, [{table, ?ROUTE_TABLE}]},
                        rabbit_misc:table_lookup(Headers, <<"x-received-from">>))
      end},
     {"disparate invalid header entries are accumulated separately",
      begin
          BadHeaders = [{<<"x-received-from">>,
                         longstr, <<"this should be a table!!!">>}],
          Headers = rabbit_basic:prepend_table_header(
                      <<"x-received-from">>, ?ROUTE_TABLE, BadHeaders),
          BadHeaders2 = rabbit_basic:prepend_table_header(
                          <<"x-death">>, ?XDEATH_TABLE,
                          [{<<"x-death">>,
                            longstr, <<"and so should this!!!">>}|Headers]),
          ?_assertMatch(
             {table,
              [{<<"x-death">>, array, [{longstr, <<"and so should this!!!">>}]},
               {<<"x-received-from">>,
                array, [{longstr, <<"this should be a table!!!">>}]}]},
             rabbit_misc:table_lookup(BadHeaders2, ?INVALID_HEADERS_KEY))
      end},
     {"corrupt or invalid x-invalid-headers entries are overwritten!",
      begin
          Headers0 = [{<<"x-death">>, longstr, <<"this should be a table">>},
                        {?INVALID_HEADERS_KEY, longstr, <<"what the!?">>}],
          Headers1 = rabbit_basic:prepend_table_header(
                      <<"x-death">>, ?XDEATH_TABLE, Headers0),
          ?_assertMatch(
             {table,
              [{<<"x-death">>, array,
                [{longstr, <<"this should be a table">>}]},
               {?INVALID_HEADERS_KEY, array,
                [{longstr, <<"what the!?">>}]}]},
             rabbit_misc:table_lookup(Headers1, ?INVALID_HEADERS_KEY))
      end}].

invalid_same_header_entry_accumulation_test() ->
    Key = <<"x-received-from">>,
    BadHeader1 = {longstr, <<"this should be a table!!!">>},
    Headers = check_invalid(Key, BadHeader1, ?ROUTE_TABLE),
    Headers2 = rabbit_basic:prepend_table_header(
                 Key,
                 ?ROUTE_TABLE,
                 [{Key, longstr,
                   <<"this should also be a table!!!">>}|Headers]),
    Invalid = rabbit_misc:table_lookup(Headers2, ?INVALID_HEADERS_KEY),
    ?assertMatch({table, _}, Invalid),
    {table, InvalidHeaders} = Invalid,
    ?assertMatch({array,
                  [{longstr, <<"this should also be a table!!!">>},BadHeader1]},
                 rabbit_misc:table_lookup(InvalidHeaders, Key)).

assert_invalid(HeaderKey, Entry, Table) ->
    fun() -> check_invalid(HeaderKey, Entry, Table) end.

check_invalid(HeaderKey, {TBin, VBin}=InvalidEntry, HeaderTable) ->
    Headers = rabbit_basic:prepend_table_header(HeaderKey, HeaderTable,
                                               [{HeaderKey, TBin, VBin}]),
    InvalidHeaders = rabbit_misc:table_lookup(Headers, ?INVALID_HEADERS_KEY),
    ?assertMatch({table, _}, InvalidHeaders),
    {_, Invalid} = InvalidHeaders,
    InvalidArrayForKey = rabbit_misc:table_lookup(Invalid, HeaderKey),
    ?assertMatch({array, _}, InvalidArrayForKey),
    {_, Array} = InvalidArrayForKey,
    ?assertMatch([InvalidEntry], Array),
    Headers.
