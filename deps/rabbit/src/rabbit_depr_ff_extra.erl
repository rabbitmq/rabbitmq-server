%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023-2024 Broadcom. All Rights Reserved. The term “Broadcom”
%% refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%
%% @doc
%% This module provides extra functions unused by the feature flags
%% subsystem core functionality.

-module(rabbit_depr_ff_extra).

-export([cli_info/1]).

-type cli_info() :: [cli_info_entry()].
%% A list of deprecated feature properties, formatted for the RabbitMQ CLI.

-type cli_info_entry() ::
        #{name => rabbit_feature_flags:feature_name(),
          deprecation_phase => rabbit_deprecated_features:deprecation_phase(),
          provided_by => atom(),
          desc => string(),
          doc_url => string()}.
%% A list of properties for a single deprecated feature, formatted for the
%% RabbitMQ CLI.

-spec cli_info(Which) -> CliInfo when
      Which :: all | used,
      CliInfo :: cli_info().
%% @doc
%% Returns a list of all or used deprecated features properties,
%% depending on the argument.
%%
%% @param Which The group of deprecated features to return: `all' or `used'.
%% @returns the list of all deprecated feature properties.

cli_info(all) ->
    cli_info0(rabbit_deprecated_features:list(all));
cli_info(used) ->
    cli_info0(rabbit_deprecated_features:list(used)).

-spec cli_info0(FeatureFlags) -> CliInfo when
      FeatureFlags :: rabbit_feature_flags:feature_flags(),
      CliInfo :: cli_info().
%% @doc
%% Formats a map of deprecated features and their properties into a list of
%% deprecated feature properties as expected by the RabbitMQ CLI.
%%
%% @param DeprecatedFeatures A map of deprecated features.
%% @returns the list of deprecated features properties, created from the map
%%   specified in arguments.

cli_info0(DeprecatedFeature) ->
    lists:foldr(
      fun(FeatureName, Acc) ->
              FeatureProps = maps:get(FeatureName, DeprecatedFeature),

              App = maps:get(provided_by, FeatureProps),
              DeprecationPhase = maps:get(deprecation_phase, FeatureProps, ""),
              Desc = maps:get(desc, FeatureProps, ""),
              DocUrl = maps:get(doc_url, FeatureProps, ""),
              Info = #{name => FeatureName,
                       desc => unicode:characters_to_binary(Desc),
                       deprecation_phase => DeprecationPhase,
                       doc_url => unicode:characters_to_binary(DocUrl),
                       provided_by => App},
              [Info | Acc]
      end, [], lists:sort(maps:keys(DeprecatedFeature))).
