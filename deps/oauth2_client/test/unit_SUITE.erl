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
-include_lib("public_key/include/public_key.hrl").

-compile(export_all).


all() ->
[
   {group, ssl_options}
].

groups() ->
[
  {ssl_options, [], [
    no_ssl_options_triggers_verify_peer,
    peer_verification_verify_none,
    peer_verification_verify_peer_with_cacertfile
  ]}
].

no_ssl_options_triggers_verify_peer(_) ->
  ?assertMatch([
    {verify, verify_peer},
    {depth, 10},
    {crl_check,false},
    {fail_if_no_peer_cert,false},
    {cacerts, _CaCerts}
  ], oauth2_client:extract_ssl_options_as_list(#{})).

peer_verification_verify_none(_) ->
  Expected1 = [
    {verify, verify_none}
  ],
  ?assertEqual(Expected1, oauth2_client:extract_ssl_options_as_list(#{peer_verification => verify_none})),

  Expected2 = [
    {verify, verify_none}
  ],
  ?assertEqual(Expected2, oauth2_client:extract_ssl_options_as_list(#{
    peer_verification => verify_none,
    cacertfile => "/tmp"
  })).


peer_verification_verify_peer_with_cacertfile(_) ->
  Expected = [
    {verify, verify_peer},
    {depth, 10},
    {crl_check,false},
    {fail_if_no_peer_cert,false},
    {cacertfile, "/tmp"}
  ],
  ?assertEqual(Expected, oauth2_client:extract_ssl_options_as_list(#{
    cacertfile => "/tmp",
    peer_verification => verify_peer
  })).
