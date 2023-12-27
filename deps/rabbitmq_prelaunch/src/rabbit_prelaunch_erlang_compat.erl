-module(rabbit_prelaunch_erlang_compat).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([check/1]).

%% minimum Erlang/OTP version supported
-define(OTP_MINIMUM, "26.0").
%% the ERTS version provided by the minimum Erlang/OTP version supported
-define(ERTS_MINIMUM, "14.0").

check(_Context) ->
    ?LOG_DEBUG(
       "~n== Erlang/OTP compatibility check ==", [],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),

    ERTSVer = erlang:system_info(version),
    OTPRel = rabbit_misc:otp_release(),
    ?LOG_DEBUG(
      "Requiring: Erlang/OTP ~ts (ERTS ~ts)~n"
      "Running:   Erlang/OTP ~ts (ERTS ~ts)",
      [?OTP_MINIMUM, ?ERTS_MINIMUM, OTPRel, ERTSVer],
      #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),

    case rabbit_misc:version_compare(?ERTS_MINIMUM, ERTSVer, lte) of
        true when ?ERTS_MINIMUM =/= ERTSVer ->
            ?LOG_DEBUG(
              "Erlang/OTP version requirement satisfied", [],
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            ok;
        true when ?ERTS_MINIMUM =:= ERTSVer andalso ?OTP_MINIMUM =< OTPRel ->
            %% When a critical regression or bug is found, a new OTP
            %% release can be published without changing the ERTS
            %% version. For instance, this is the case with R16B03 and
            %% R16B03-1.
            %%
            %% In this case, we compare the release versions
            %% alphabetically.
            ok;
        _ ->
            Msg =
            "This RabbitMQ version cannot run on Erlang ~ts (erts ~ts): "
            "minimum required version is ~ts (erts ~ts)",
            Args = [OTPRel, ERTSVer, ?OTP_MINIMUM, ?ERTS_MINIMUM],
            ?LOG_ERROR(Msg, Args, #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),

            %% Also print to stderr to make this more visible
            io:format(standard_error, "Error: " ++ Msg ++ "~n", Args),

            Msg2 = rabbit_misc:format(
                     "Erlang ~ts or later is required, started on ~ts",
                     [?OTP_MINIMUM, OTPRel]),
            throw({error, {erlang_version_too_old, Msg2}})
    end.
