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

write_table_with_invalid_existing_type_test() ->
    assertInvalid(<<"x-death">>, {longstr, <<"this should be a table!!!">>},
                  ?XDEATH_TABLE).

assertInvalid(HeaderKey, {TBin, VBin}=InvalidEntry, HeaderTable) ->
    Headers = rabbit_basic:append_table_header(HeaderKey, HeaderTable,
                                               [{HeaderKey, TBin, VBin}]),
    InvalidHeaders = rabbit_misc:table_lookup(Headers, ?INVALID_HEADERS_KEY),
    ?assertMatch({table, _}, InvalidHeaders),
    {_, Invalid} = InvalidHeaders,
    InvalidArrayForKey = rabbit_misc:table_lookup(Invalid, HeaderKey),
    ?assertMatch({array, _}, InvalidArrayForKey),
    {_, Array} = InvalidArrayForKey,
    ?assertMatch([InvalidEntry], Array).

