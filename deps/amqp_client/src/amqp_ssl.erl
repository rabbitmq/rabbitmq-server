-module(amqp_ssl).

-include("amqp_client_internal.hrl").

-include_lib("public_key/include/public_key.hrl").

-export([maybe_enhance_ssl_options/1,
         add_verify_fun_to_opts/2,
         verify_fun/3]).

maybe_enhance_ssl_options(Params = #amqp_params_network{ssl_options = none}) ->
    Params;
maybe_enhance_ssl_options(Params = #amqp_params_network{host = Host, ssl_options = Opts0}) ->
    Opts1 = maybe_add_sni(Host, Opts0),
    Opts2 = maybe_add_verify(Opts1),
    Params#amqp_params_network{ssl_options = Opts2};
maybe_enhance_ssl_options(Params) ->
    Params.

% https://github.com/erlang/otp/blob/master/lib/inets/src/http_client/httpc_handler.erl
maybe_add_sni(Host, Options) ->
    maybe_add_sni_0(lists:keyfind(server_name_indication, 1, Options), Host, Options).

maybe_add_sni_0(false, Host, Options) ->
    % NB: this is the case where the user did not specify
    % server_name_indication at all. If Host is a DNS host name,
    % we will specify server_name_indication via code
    maybe_add_sni_1(inet_parse:domain(Host), Host, Options);
maybe_add_sni_0({server_name_indication, disable}, _Host, Options) ->
    % NB: this is the case where the user explicitly disabled
    % server_name_indication
    Options;
maybe_add_sni_0({server_name_indication, SniHost}, _Host, Options) ->
    % NB: this is the case where the user explicitly specified
    % an SNI host name. We may need to add verify_fun for OTP 19
    maybe_add_verify_fun(lists:keymember(verify_fun, 1, Options), SniHost, Options).

maybe_add_sni_1(false, _Host, Options) ->
    % NB: host is not a DNS host name, so nothing to add
    Options;
maybe_add_sni_1(true, Host, Options) ->
    Opts1 = [{server_name_indication, Host} | Options],
    maybe_add_verify_fun(lists:keymember(verify_fun, 1, Opts1), Host, Opts1).

maybe_add_verify_fun(true, _Host, Options) ->
    % NB: verify_fun already present, don't add twice
    Options;
maybe_add_verify_fun(false, Host, Options) ->
    add_verify_fun_to_opts(lists:keyfind(verify, 1, Options), Host, Options).

maybe_add_verify(Options) ->
    case lists:keymember(verify, 1, Options) of
        true ->
            % NB: user has explicitly set 'verify'
            Options;
        _ ->
            ?LOG_WARN("Connection (~p): Certificate chain verification is not enabled for this TLS connection. "
                    "Please see https://rabbitmq.com/ssl.html for more information.~n", [self()]),
            Options
    end.
    % TODO FUTURE 3.8.x
    % verify_peer will become the default in RabbitMQ 3.8.0
    %     false ->
    %         [{verify, verify_peer} | Options]
    % end.

add_verify_fun_to_opts(Host, Options) ->
    add_verify_fun_to_opts(false, Host, Options).

add_verify_fun_to_opts({verify, verify_none}, _Host, Options) ->
    % NB: this is the case where the user explicitly disabled
    % certificate chain verification so there's not much sense
    % in adding verify_fun
    Options;
add_verify_fun_to_opts(_, Host, Options) ->
    % NB: this is the case where the user either did not
    % set the verify option or set it to verify_peer
    case erlang:system_info(otp_release) of
        "19" ->
            F = fun ?MODULE:verify_fun/3,
            [{verify_fun, {F, Host}} | Options];
        _ -> Options
    end.

-type hostname() :: nonempty_string() | binary().

-spec verify_fun(Cert :: #'OTPCertificate'{},
                 Event :: {bad_cert, Reason :: atom() | {revoked, atom()}} |
                          {extension, #'Extension'{}}, InitialUserState :: term()) ->
                    {valid, UserState :: term()} | {valid_peer, UserState :: hostname()} |
                    {fail, Reason :: term()} | {unknown, UserState :: term()}.
verify_fun(_, {bad_cert, _} = Reason, _) ->
    {fail, Reason};
verify_fun(_, {extension, _}, UserState) ->
    {unknown, UserState};
verify_fun(_, valid, UserState) ->
    {valid, UserState};
% NOTE:
% The user state is the hostname to verify as configured in
% amqp_ssl:make_verify_fun
verify_fun(Cert, valid_peer, Hostname) when Hostname =/= disable ->
    verify_hostname(Cert, Hostname);
verify_fun(_, valid_peer, UserState) ->
    {valid, UserState}.

% https://github.com/erlang/otp/blob/master/lib/ssl/src/ssl_certificate.erl
verify_hostname(Cert, Hostname) ->
    case public_key:pkix_verify_hostname(Cert, [{dns_id, Hostname}]) of
        true ->
            {valid, Hostname};
        false ->
            {fail, {bad_cert, hostname_check_failed}}
    end.
