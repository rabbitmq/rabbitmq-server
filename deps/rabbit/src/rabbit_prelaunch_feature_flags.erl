%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
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
            ?LOG_DEBUG(
               "Initializing feature flags registry", [],
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            case rabbit_feature_flags:initialize_registry() of
                ok ->
                    ok;
                {error, Reason} ->
                    ?LOG_ERROR(
                      "Failed to initialize feature flags registry: ~p",
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
