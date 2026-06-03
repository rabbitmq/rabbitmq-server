-module(conflicting_dummy_interceptor).

-behaviour(rabbit_channel_interceptor).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-compile(export_all).

init(_Ch) ->
    undefined.

description() ->
    [{description,
      <<"Interceptor that conflicts with dummy_interceptor on queue.declare">>}].

intercept(Method, Content, _IState) ->
    {Method, Content}.

applies_to() ->
    ['queue.declare'].
