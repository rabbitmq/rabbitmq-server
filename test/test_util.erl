-module(test_util).

-export([
         fake_pid/1
         ]).


fake_pid(Node) ->
    NodeBin = rabbit_data_coercion:to_binary(Node),
    ThisNodeSize = size(term_to_binary(node())) + 1,
    Pid = spawn(fun () -> ok end),
    %% drop the local node data from a local pid
    <<Pre:ThisNodeSize/binary, LocalPidData/binary>> = term_to_binary(Pid),
    S = size(NodeBin),
    %% get the encoding type of the pid
    <<_:8, Type:8/unsigned, _/binary>> = Pre,
    %% replace it with the incoming node binary
    Final = <<131, Type, 100, S:16/unsigned, NodeBin/binary, LocalPidData/binary>>,
    binary_to_term(Final).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

fake_pid_test() ->
    _ = fake_pid(banana),
    ok.

-endif.
