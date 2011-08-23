-module(rabbit_amqp1_0_session).

-export([process_frame/2]).

process_frame(Pid, Frame) ->
    gen_server2:cast(Pid, {frame, Frame}).
