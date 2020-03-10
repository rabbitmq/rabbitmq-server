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

-module(rabbit_ssl).

-include_lib("public_key/include/public_key.hrl").

-export([peer_cert_issuer/1, peer_cert_subject/1, peer_cert_validity/1]).
-export([peer_cert_subject_items/2, peer_cert_auth_name/1]).
-export([cipher_suites_erlang/2, cipher_suites_erlang/1,
         cipher_suites_openssl/2, cipher_suites_openssl/1,
         cipher_suites/1]).

%%--------------------------------------------------------------------------

-export_type([certificate/0]).

% Due to API differences between OTP releases.
-dialyzer(no_missing_calls).
-ignore_xref([{ssl_cipher_format, suite_legacy, 1},
              {ssl_cipher_format, suite, 1},
              {ssl_cipher_format, suite_to_str, 1},
              {ssl_cipher_format, erl_suite_definition, 1},
              {ssl_cipher_format, suite_map_to_openssl_str, 1},
              {ssl_cipher_format, suite_map_to_bin, 1}]).

-type certificate() :: rabbit_cert_info:certificate().

-type cipher_suites_mode() :: default | all | anonymous.

-spec cipher_suites(cipher_suites_mode()) -> ssl:ciphers().
cipher_suites(Mode) ->
    Version = get_highest_protocol_version(),
    ssl:cipher_suites(Mode, Version).

-spec cipher_suites_erlang(cipher_suites_mode()) ->
    [ssl:old_cipher_suite()].
cipher_suites_erlang(Mode) ->
    Version = get_highest_protocol_version(),
    cipher_suites_erlang(Mode, Version).

-spec cipher_suites_erlang(cipher_suites_mode(),
                           ssl:protocol_version() | tls_record:tls_version()) ->
    [ssl:old_cipher_suite()].
cipher_suites_erlang(Mode, Version) ->
    [ format_cipher_erlang(C)
      || C <- ssl:cipher_suites(Mode, Version) ].

-spec cipher_suites_openssl(cipher_suites_mode()) ->
    [ssl:old_cipher_suite()].
cipher_suites_openssl(Mode) ->
    Version = get_highest_protocol_version(),
    cipher_suites_openssl(Mode, Version).

-spec cipher_suites_openssl(cipher_suites_mode(),
                           ssl:protocol_version() | tls_record:tls_version()) ->
    [ssl:old_cipher_suite()].
cipher_suites_openssl(Mode, Version) ->
    lists:filtermap(fun(C) ->
        OpenSSL = format_cipher_openssl(C),
        case is_list(OpenSSL) of
            true  -> {true, OpenSSL};
            false -> false
        end
    end,
    ssl:cipher_suites(Mode, Version)).


format_cipher_erlang(Cipher) ->
  case erlang:function_exported(ssl_cipher_format, suite_map_to_bin, 1) of
      true ->
          format_cipher_erlang22(Cipher);
      false ->
          format_cipher_erlang21(Cipher)
  end.

format_cipher_erlang22(Cipher) ->
  ssl_cipher_format:suite_legacy(ssl_cipher_format:suite_map_to_bin(Cipher)).

format_cipher_erlang21(Cipher) ->
  ssl_cipher_format:erl_suite_definition(ssl_cipher_format:suite(Cipher)).


format_cipher_openssl(Cipher) ->
    case erlang:function_exported(ssl_cipher_format, suite_map_to_bin, 1) of
      true ->
        format_cipher_openssl22(Cipher);
      false ->
        format_cipher_openssl21(Cipher)
    end.

format_cipher_openssl22(Cipher) ->
    ssl_cipher_format:suite_map_to_openssl_str(Cipher).

format_cipher_openssl21(Cipher) ->
    ssl_cipher_format:suite_to_str(Cipher).

-spec get_highest_protocol_version() -> tls_record:tls_version().
get_highest_protocol_version() ->
    tls_record:highest_protocol_version([]).

%%--------------------------------------------------------------------------
%% High-level functions used by reader
%%--------------------------------------------------------------------------

%% Return a string describing the certificate's issuer.
peer_cert_issuer(Cert) ->
    rabbit_cert_info:issuer(Cert).

%% Return a string describing the certificate's subject, as per RFC4514.
peer_cert_subject(Cert) ->
    rabbit_cert_info:subject(Cert).

%% Return the parts of the certificate's subject.
peer_cert_subject_items(Cert, Type) ->
    rabbit_cert_info:subject_items(Cert, Type).

%% Return a string describing the certificate's validity.
peer_cert_validity(Cert) ->
    rabbit_cert_info:validity(Cert).

%% Extract a username from the certificate
-spec peer_cert_auth_name
        (certificate()) -> binary() | 'not_found' | 'unsafe'.

peer_cert_auth_name(Cert) ->
    {ok, Mode} = application:get_env(rabbit, ssl_cert_login_from),
    peer_cert_auth_name(Mode, Cert).

peer_cert_auth_name(distinguished_name, Cert) ->
    case auth_config_sane() of
        true  -> iolist_to_binary(peer_cert_subject(Cert));
        false -> unsafe
    end;

peer_cert_auth_name(common_name, Cert) ->
    %% If there is more than one CN then we join them with "," in a
    %% vaguely DN-like way. But this is more just so we do something
    %% more intelligent than crashing, if you actually want to escape
    %% things properly etc, use DN mode.
    case auth_config_sane() of
        true  -> case peer_cert_subject_items(Cert, ?'id-at-commonName') of
                     not_found -> not_found;
                     CNs       -> list_to_binary(string:join(CNs, ","))
                 end;
        false -> unsafe
    end.

auth_config_sane() ->
    {ok, Opts} = application:get_env(rabbit, ssl_options),
    case proplists:get_value(verify, Opts) of
        verify_peer -> true;
        V           -> rabbit_log:warning("SSL certificate authentication "
                                          "disabled, verify=~p~n", [V]),
                       false
    end.
