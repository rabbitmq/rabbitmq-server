%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("oauth2_client.hrl").

-compile(export_all).


all() ->
[
   {group, ssl_options}
].

groups() ->
[
  {ssl_options, [], [
    no_ssl_options_set
  ]}
].

no_ssl_options_set(_) ->
  Map = #{ },
  ?assertEqual([
    {verify, verify_none}
  ], oauth2_client:extract_ssl_options_as_list(Map)).
