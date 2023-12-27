-module(dummy_interceptor).

-behaviour(rabbit_channel_interceptor).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").


-compile(export_all).

init(_Ch) ->
    undefined.

description() ->
    [{description,
      <<"Empties payload on publish">>}].

intercept(#'basic.publish'{} = Method, Content, _IState) ->
    Content2 = Content#content{payload_fragments_rev = []},
    {Method, Content2};

%% Use 'queue.declare' to test #amqp_error{} handling
intercept(#'queue.declare'{queue = <<"failing-with-amqp-error-q">>}, _Content, _IState) ->
    rabbit_misc:amqp_error(
        'precondition_failed', "operation not allowed", [],
        'queue.declare');

intercept(#'queue.declare'{queue = QName = <<"crashing-with-amqp-exception-q">>}, _Content, _IState) ->
    QRes = rabbit_misc:r(<<"/">>, queue, QName),
    rabbit_misc:assert_field_equivalence(true, false, QRes, durable);

intercept(Method, Content, _VHost) ->
    {Method, Content}.

applies_to() ->
    ['basic.publish', 'queue.declare'].
