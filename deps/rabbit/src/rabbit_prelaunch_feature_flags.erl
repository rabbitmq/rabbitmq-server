%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_prelaunch_feature_flags).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([setup/1]).

setup(#{feature_flags_file := FFFile}) ->
    ?LOG_DEBUG(
       "~n== Feature flags ==", [],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    case filelib:ensure_dir(FFFile) of
        ok ->
            %% On boot, we know that there should be no registry loaded at
            %% first. There could be a loaded registry around during a
            %% stop_app/start_app, so reset it here. This ensures that e.g.
            %% any change to the configuration file w.r.t. deprecated features
            %% are taken into account.
            rabbit_feature_flags:reset_registry(),

            ?LOG_DEBUG(
               "Initializing feature flags registry", [],
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            case rabbit_ff_registry_factory:initialize_registry() of
                ok ->
                    ok;
                {error, Reason} ->
                    ?LOG_ERROR(
                      "Failed to initialize feature flags registry: ~tp",
                      [Reason],
                      #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                    throw({error, failed_to_initialize_feature_flags_registry})
            end;
        {error, Reason} ->
            ?LOG_ERROR(
              "Failed to create feature flags file \"~ts\" directory: ~ts",
              [FFFile, file:format_error(Reason)],
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            throw({error, failed_to_create_feature_flags_file_directory})
    end.
