-module(rabbit_tracing_util).

-export([coerce_env_value/2]).

coerce_env_value(username, Val) -> rabbit_data_coercion:to_binary(Val);
coerce_env_value(password, Val) -> rabbit_data_coercion:to_binary(Val);
coerce_env_value(_,        Val) -> Val.
