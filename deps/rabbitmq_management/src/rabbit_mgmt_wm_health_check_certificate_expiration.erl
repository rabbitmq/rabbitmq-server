%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% An HTTP API counterpart of 'rabbitmq-diagnostics check_certificate_expiration'
-module(rabbit_mgmt_wm_health_check_certificate_expiration).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([resource_exists/2]).
-export([variances/2]).

-include_lib("public_key/include/public_key.hrl").
-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(DAYS_SECONDS, 86400).
-define(WEEKS_SECONDS, ?DAYS_SECONDS * 7).
-define(MONTHS_SECONDS, ?DAYS_SECONDS * (365 / 12)).
-define(YEARS_SECONDS, ?DAYS_SECONDS * 365).

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData, Context) ->
    {true, ReqData, Context}.

to_json(ReqData, Context) ->
    Listeners = rabbit_networking:active_listeners(),
    Local = [L || #listener{node = N} = L <- Listeners, N == node()],
    case convert(within(ReqData), unit(ReqData)) of
        {error, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context);
        Seconds ->
            ExpiringListeners = lists:foldl(fun(L, Acc) ->
                                                    case listener_expiring_within(L, Seconds) of
                                                        false -> Acc;
                                                        Map -> [Map | Acc]
                                                    end
                                            end, [], Local),
            case ExpiringListeners of
                [] ->
                    rabbit_mgmt_util:reply(#{status => ok}, ReqData, Context);
                _ ->
                    Msg = <<"Certificates expiring">>,
                    failure(Msg, ExpiringListeners, ReqData, Context)
            end
    end.

failure(Message, Listeners, ReqData, Context) ->
    Body = #{
        status  => failed,
        reason  => Message,
        expired => Listeners
    },
    {Response, ReqData1, Context1} = rabbit_mgmt_util:reply(Body, ReqData, Context),
    {stop, cowboy_req:reply(503, #{}, Response, ReqData1), Context1}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

within(ReqData) ->
    rabbit_mgmt_util:id(within, ReqData).

unit(ReqData) ->
    rabbit_mgmt_util:id(unit, ReqData).

convert(Time, Unit) ->
    try
        do_convert(binary_to_integer(Time), string:lowercase(binary_to_list(Unit)))
    catch
        error:badarg ->
            {error, "Invalid expiration value."};
        invalid_unit ->
            {error, "Time unit not recognised. Supported units: days, weeks, months, years."}
    end.

do_convert(Time, "days") ->
    Time * ?DAYS_SECONDS;
do_convert(Time, "weeks") ->
    Time * ?WEEKS_SECONDS;
do_convert(Time, "months") ->
    Time * ?MONTHS_SECONDS;
do_convert(Time, "years") ->
    Time * ?YEARS_SECONDS;
do_convert(_, _) ->
    throw(invalid_unit).

listener_expiring_within(#listener{node = Node, protocol = Protocol, ip_address = Interface,
                                   port = Port, opts = Opts}, Seconds) ->
    Certfile = proplists:get_value(certfile, Opts),
    Cacertfile = proplists:get_value(cacertfile, Opts),
    Now = calendar:datetime_to_gregorian_seconds(calendar:universal_time()),
    ExpiryDate = Now + Seconds,
    CertfileExpiresOn = expired(cert_validity(read_cert(Certfile)), ExpiryDate),
    CacertfileExpiresOn = expired(cert_validity(read_cert(Cacertfile)), ExpiryDate),
    case {CertfileExpiresOn, CacertfileExpiresOn} of
        {[], []} ->
            false;
        _ ->
            #{node => Node,
              protocol => Protocol,
              interface => rabbit_data_coercion:to_utf8_binary(rabbit_misc:ntoa(Interface)),
              port => Port,
              certfile => list_to_binary(Certfile),
              cacertfile => list_to_binary(Cacertfile),
              certfile_expires_on => expires_on_list(CertfileExpiresOn),
              cacertfile_expires_on => expires_on_list(CacertfileExpiresOn)
             }
    end.

expires_on_list({error, Reason}) ->
    #{error => format_error_reason(Reason)};
expires_on_list(ExpiresOn) when is_list(ExpiresOn) ->
    [case S of
         {error, Reason} ->
             #{error => format_error_reason(Reason)};
         Seconds when is_integer(Seconds) ->
             seconds_to_bin(Seconds)
     end || S <- ExpiresOn].

format_error_reason(Reason) when is_binary(Reason) ->
    Reason;
format_error_reason(Reason) ->
    iolist_to_binary(Reason).

read_cert(undefined) ->
    undefined;
read_cert({pem, Pem}) ->
    Pem;
read_cert(Path) ->
    case rabbit_misc:raw_read_file(Path) of
        {ok, Bin} ->
            Bin;
        Err ->
            Err
    end.

cert_validity(undefined) ->
    undefined;
cert_validity({error, _} = Err) ->
    Err;
cert_validity(Cert) ->
    DsaEntries = public_key:pem_decode(Cert),
    case DsaEntries of
        [] ->
            {error, "The certificate file provided does not contain any PEM entry."};
        _ ->
            Now = calendar:datetime_to_gregorian_seconds(calendar:universal_time()),
            lists:map(
              fun({'Certificate', _, _} = DsaEntry) ->
                      #'Certificate'{tbsCertificate = TBSCertificate} = public_key:pem_entry_decode(DsaEntry),
                      #'TBSCertificate'{validity = Validity} = TBSCertificate,
                      #'Validity'{notAfter = NotAfter, notBefore = NotBefore} = Validity,
                      case parse_time(NotBefore) of
                          {error, _} = Err ->
                              Err;
                          Start when Start > Now ->
                              {error, "Certificate is not yet valid"};
                          _Start ->
                              case parse_time(NotAfter) of
                                  {error, _} = Err ->
                                      Err;
                                  End ->
                                      End
                              end
                      end;
                 ({Type, _, _}) ->
                      {error, io_lib:format("The certificate file provided contains a ~tp entry",
                                            [Type])}
              end, DsaEntries)
    end.

parse_time(Time) ->
    try
        time_str_2_gregorian_sec(Time)
    catch
        _:_ ->
            {error, "Invalid date format in certificate"}
    end.

%% RFC 5280 Section 4.1.2.5.1: YY >= 50 is interpreted as 19YY, YY < 50 as 20YY.
time_str_2_gregorian_sec({utcTime, [Y1, Y2, M1, M2, D1, D2, H1, H2, M3, M4, S1, S2, $Z]}) ->
    YY = list_to_integer([Y1, Y2]),
    Year = if YY >= 50 -> 1900 + YY;
              true     -> 2000 + YY
           end,
    Month = list_to_integer([M1, M2]),
    Day = list_to_integer([D1, D2]),
    Hour = list_to_integer([H1, H2]),
    Min = list_to_integer([M3, M4]),
    Sec = list_to_integer([S1, S2]),
    calendar:datetime_to_gregorian_seconds({{Year, Month, Day}, {Hour, Min, Sec}});
time_str_2_gregorian_sec({utcTime, [Y1, Y2, M1, M2, D1, D2, H1, H2, M3, M4, $Z]}) ->
    time_str_2_gregorian_sec({utcTime, [Y1, Y2, M1, M2, D1, D2, H1, H2, M3, M4, $0, $0, $Z]});
time_str_2_gregorian_sec(Time) ->
    pubkey_cert:time_str_2_gregorian_sec(Time).

expired(undefined, _ExpiryDate) ->
    [];
expired({error, _} = Error, _ExpiryDate) ->
    Error;
expired(Expires, ExpiryDate) ->
    lists:filter(fun({error, _}) ->
                         true;
                    (Seconds) ->
                         Seconds < ExpiryDate
                 end, Expires).

seconds_to_bin(Seconds) ->
    {{Y, M, D}, {H, Min, S}} = calendar:gregorian_seconds_to_datetime(Seconds),
    list_to_binary(lists:flatten(io_lib:format("~w-~2.2.0w-~2.2.0w ~w:~2.2.0w:~2.2.0w",
                                               [Y, M, D, H, Min, S]))).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

rfc5280_utctime_test() ->
    Sec1970 = time_str_2_gregorian_sec({utcTime, "700101000000Z"}),
    ?assertEqual({{1970, 1, 1}, {0, 0, 0}}, calendar:gregorian_seconds_to_datetime(Sec1970)),
    Sec2049 = time_str_2_gregorian_sec({utcTime, "491231235959Z"}),
    ?assertEqual({{2049, 12, 31}, {23, 59, 59}}, calendar:gregorian_seconds_to_datetime(Sec2049)).

expires_on_list_error_tuple_test() ->
    Error = {error, "Certificate is not yet valid"},
    ?assertEqual([#{error => <<"Certificate is not yet valid">>}], expires_on_list([Error])).

-endif.
