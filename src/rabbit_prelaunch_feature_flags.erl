%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_prelaunch_feature_flags).

-export([setup/1]).

setup(#{feature_flags_file := FFFile}) ->
    rabbit_log_prelaunch:debug(""),
    rabbit_log_prelaunch:debug("== Feature flags =="),
    case filelib:ensure_dir(FFFile) of
        ok ->
            rabbit_log_prelaunch:debug("Initializing feature flags registry"),
            case rabbit_feature_flags:initialize_registry() of
                ok ->
                    ok;
                {error, Reason} ->
                    rabbit_log_prelaunch:error(
                      "Failed to initialize feature flags registry: ~p",
                      [Reason]),
                    throw({error, failed_to_initialize_feature_flags_registry})
            end;
        {error, Reason} ->
            rabbit_log_prelaunch:error(
              "Failed to create feature flags file \"~ts\" directory: ~ts",
              [FFFile, file:format_error(Reason)]),
            throw({error, failed_to_create_feature_flags_file_directory})
    end.
