-module(uaa_jwt_jwt).

%% Transitional step until we can require Erlang/OTP 21 and
%% use the now recommended try/catch syntax for obtaining the stack trace.
-compile(nowarn_deprecated_function).

-export([decode/1, decode_and_verify/2, get_key_id/1]).

-include_lib("jose/include/jose_jwt.hrl").
-include_lib("jose/include/jose_jws.hrl").

decode(Token) ->
    try
        #jose_jwt{fields = Fields} = jose_jwt:peek_payload(Token),
        Fields
    catch Type:Err ->
        {error, {invalid_token, Type, Err, erlang:get_stacktrace()}}
    end.

decode_and_verify(Jwk, Token) ->
    case jose_jwt:verify(Jwk, Token) of
        {true, #jose_jwt{fields = Fields}, _}  -> {true, Fields};
        {false, #jose_jwt{fields = Fields}, _} -> {false, Fields}
    end.

get_key_id(Token) ->
    try
        case jose_jwt:peek_protected(Token) of
            #jose_jws{fields = #{<<"kid">> := Kid}} -> {ok, Kid};
            #jose_jws{}                             -> get_default_key()
        end
    catch Type:Err ->
        {error, {invalid_token, Type, Err, erlang:get_stacktrace()}}
    end.


get_default_key() ->
    UaaEnv = application:get_env(rabbitmq_auth_backend_oauth2, uaa_jwt, []),
    case proplists:get_value(default_key, UaaEnv, undefined) of
        undefined -> {error, no_key};
        Val       -> {ok, Val}
    end.
