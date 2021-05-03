-module(rabbit_prelaunch_erlang_compat).

-export([check/1]).

-define(OTP_MINIMUM, "23.2").
-define(ERTS_MINIMUM, "11.0").

check(_Context) ->
    _ = rabbit_log_prelaunch:debug(""),
    _ = rabbit_log_prelaunch:debug("== Erlang/OTP compatibility check =="),

    ERTSVer = erlang:system_info(version),
    OTPRel = rabbit_misc:otp_release(),
    _ = rabbit_log_prelaunch:debug(
      "Requiring: Erlang/OTP ~s (ERTS ~s)", [?OTP_MINIMUM, ?ERTS_MINIMUM]),
    _ = rabbit_log_prelaunch:debug(
      "Running:   Erlang/OTP ~s (ERTS ~s)", [OTPRel, ERTSVer]),

    case rabbit_misc:version_compare(?ERTS_MINIMUM, ERTSVer, lte) of
        true when ?ERTS_MINIMUM =/= ERTSVer ->
            _ = rabbit_log_prelaunch:debug(
              "Erlang/OTP version requirement satisfied"),
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
            "This RabbitMQ version cannot run on Erlang ~s (erts ~s): "
            "minimum required version is ~s (erts ~s)",
            Args = [OTPRel, ERTSVer, ?OTP_MINIMUM, ?ERTS_MINIMUM],
            _ = rabbit_log_prelaunch:error(Msg, Args),

            %% Also print to stderr to make this more visible
            io:format(standard_error, "Error: " ++ Msg ++ "~n", Args),

            Msg2 = rabbit_misc:format(
                     "Erlang ~s or later is required, started on ~s",
                     [?OTP_MINIMUM, OTPRel]),
            throw({error, {erlang_version_too_old, Msg2}})
    end.
