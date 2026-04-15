%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_definitions_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          pick_known_metadata_with_binary_keys,
          pick_known_metadata_with_atom_keys,
          pick_known_metadata_drops_unknown_keys,
          pick_known_metadata_with_empty_map,
          pick_known_metadata_with_non_map,
          pick_known_metadata_prefers_binary_keys
        ]}
    ].

%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

pick_known_metadata_with_binary_keys(_Config) ->
    Input = #{<<"description">> => <<"a vhost">>,
              <<"tags">> => [<<"production">>],
              <<"default_queue_type">> => <<"quorum">>},
    Expected = #{description => <<"a vhost">>,
                 tags => [<<"production">>],
                 default_queue_type => <<"quorum">>},
    Expected = rabbit_vhost:pick_known_metadata(Input).

pick_known_metadata_with_atom_keys(_Config) ->
    Input = #{description => <<"a vhost">>,
              tags => [<<"production">>],
              default_queue_type => <<"quorum">>,
              protected_from_deletion => true},
    Input = rabbit_vhost:pick_known_metadata(Input).

pick_known_metadata_drops_unknown_keys(_Config) ->
    Input = #{<<"description">> => <<"ok">>,
              <<"injected_atom_1">> => 1,
              <<"injected_atom_2">> => 2,
              <<"some_random_key">> => true},
    #{description := <<"ok">>} = Result = rabbit_vhost:pick_known_metadata(Input),
    1 = map_size(Result).

pick_known_metadata_with_empty_map(_Config) ->
    #{} = rabbit_vhost:pick_known_metadata(#{}),
    #{} = rabbit_vhost:pick_known_metadata(undefined).

pick_known_metadata_with_non_map(_Config) ->
    #{} = rabbit_vhost:pick_known_metadata(not_a_map),
    #{} = rabbit_vhost:pick_known_metadata([{description, <<"x">>}]).

pick_known_metadata_prefers_binary_keys(_Config) ->
    Input = #{<<"description">> => <<"binary wins">>,
              description => <<"atom loses">>},
    #{description := <<"binary wins">>} = rabbit_vhost:pick_known_metadata(Input).
