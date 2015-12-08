-module(rabbit_tracing_util).

-export([to_binary/1, coerce_env_value/2]).

%% TODO: move to rabbit_common
%% TODO: move to rabbit_common
coerce_env_value(username, Val) -> to_binary(Val);
coerce_env_value(password, Val) -> to_binary(Val);
coerce_env_value(_,        Val) -> Val.

%% TODO: move to rabbit_common
to_binary(Val) when is_list(Val) -> list_to_binary(Val);
to_binary(Val)                   -> Val.
