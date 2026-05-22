%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(prop_rabbit_cuttlefish_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").

-define(ITERATIONS, 500).
-define(KEY, "x.y.password").
-define(TOKENS, ["x", "y", "password"]).

all() ->
    [
     prop_binary_plain_string_yields_binary,
     prop_binary_plain_binary_yields_same_binary,
     prop_binary_encrypted_string_yields_encrypted_binary,
     prop_binary_any_tag_normalized_to_encrypted,
     prop_binary_any_tag_with_string_value_normalized,

     prop_string_plain_string_yields_same_string,
     prop_string_plain_binary_yields_string,
     prop_string_encrypted_binary_yields_encrypted_string,
     prop_string_any_tag_normalized_to_encrypted,
     prop_string_any_tag_with_binary_value_normalized
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

non_empty_string() ->
    non_empty(list(choose($!, $~))).

non_empty_binary() ->
    non_empty(binary()).

other_tag_atom() ->
    elements([custom, foo, bar, vault, kms]).

prop_binary_plain_string_yields_binary(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun () ->
          ?FORALL(Str, non_empty_string(),
              begin
                  Conf = [{?TOKENS, Str}],
                  rabbit_cuttlefish:optionally_tagged_binary(?KEY, Conf)
                      =:= list_to_binary(Str)
              end)
      end, [], ?ITERATIONS).

prop_binary_plain_binary_yields_same_binary(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun () ->
          ?FORALL(Bin, non_empty_binary(),
              begin
                  Conf = [{?TOKENS, Bin}],
                  rabbit_cuttlefish:optionally_tagged_binary(?KEY, Conf) =:= Bin
              end)
      end, [], ?ITERATIONS).

prop_binary_encrypted_string_yields_encrypted_binary(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun () ->
          ?FORALL(Str, non_empty_string(),
              begin
                  Conf = [{?TOKENS, {encrypted, Str}}],
                  rabbit_cuttlefish:optionally_tagged_binary(?KEY, Conf)
                      =:= {encrypted, list_to_binary(Str)}
              end)
      end, [], ?ITERATIONS).

prop_binary_any_tag_normalized_to_encrypted(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun () ->
          ?FORALL({Tag, Bin}, {other_tag_atom(), non_empty_binary()},
              begin
                  Conf = [{?TOKENS, {Tag, Bin}}],
                  rabbit_cuttlefish:optionally_tagged_binary(?KEY, Conf)
                      =:= {encrypted, Bin}
              end)
      end, [], ?ITERATIONS).

prop_binary_any_tag_with_string_value_normalized(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun () ->
          ?FORALL({Tag, Str}, {other_tag_atom(), non_empty_string()},
              begin
                  Conf = [{?TOKENS, {Tag, Str}}],
                  rabbit_cuttlefish:optionally_tagged_binary(?KEY, Conf)
                      =:= {encrypted, list_to_binary(Str)}
              end)
      end, [], ?ITERATIONS).

prop_string_plain_string_yields_same_string(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun () ->
          ?FORALL(Str, non_empty_string(),
              begin
                  Conf = [{?TOKENS, Str}],
                  rabbit_cuttlefish:optionally_tagged_string(?KEY, Conf) =:= Str
              end)
      end, [], ?ITERATIONS).

prop_string_plain_binary_yields_string(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun () ->
          ?FORALL(Bin, non_empty_binary(),
              begin
                  Conf = [{?TOKENS, Bin}],
                  rabbit_cuttlefish:optionally_tagged_string(?KEY, Conf)
                      =:= binary_to_list(Bin)
              end)
      end, [], ?ITERATIONS).

prop_string_encrypted_binary_yields_encrypted_string(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun () ->
          ?FORALL(Bin, non_empty_binary(),
              begin
                  Conf = [{?TOKENS, {encrypted, Bin}}],
                  rabbit_cuttlefish:optionally_tagged_string(?KEY, Conf)
                      =:= {encrypted, binary_to_list(Bin)}
              end)
      end, [], ?ITERATIONS).

prop_string_any_tag_normalized_to_encrypted(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun () ->
          ?FORALL({Tag, Str}, {other_tag_atom(), non_empty_string()},
              begin
                  Conf = [{?TOKENS, {Tag, Str}}],
                  rabbit_cuttlefish:optionally_tagged_string(?KEY, Conf)
                      =:= {encrypted, Str}
              end)
      end, [], ?ITERATIONS).

prop_string_any_tag_with_binary_value_normalized(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun () ->
          ?FORALL({Tag, Bin}, {other_tag_atom(), non_empty_binary()},
              begin
                  Conf = [{?TOKENS, {Tag, Bin}}],
                  rabbit_cuttlefish:optionally_tagged_string(?KEY, Conf)
                      =:= {encrypted, binary_to_list(Bin)}
              end)
      end, [], ?ITERATIONS).
