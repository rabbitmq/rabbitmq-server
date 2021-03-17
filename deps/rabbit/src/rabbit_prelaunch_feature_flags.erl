%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_prelaunch_feature_flags).

-export([setup/1]).

setup(#{feature_flags_file := FFFile}) ->
    _ = rabbit_log_prelaunch:debug(""),
    _ = rabbit_log_prelaunch:debug("== Feature flags =="),
    case filelib:ensure_dir(FFFile) of
        ok ->
            _ = rabbit_log_prelaunch:debug("Initializing feature flags registry"),
            case rabbit_feature_flags:initialize_registry() of
                ok ->
                    ok;
                {error, Reason} ->
                    _ = rabbit_log_prelaunch:error(
                      "Failed to initialize feature flags registry: ~p",
                      [Reason]),
                    throw({error, failed_to_initialize_feature_flags_registry})
            end;
        {error, Reason} ->
            _ = rabbit_log_prelaunch:error(
              "Failed to create feature flags file \"~ts\" directory: ~ts",
              [FFFile, file:format_error(Reason)]),
            throw({error, failed_to_create_feature_flags_file_directory})
    end.
