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
