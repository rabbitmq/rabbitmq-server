-module(test_util).

-export([
         fake_pid/1
         ]).


fake_pid(Node) ->
    NodeBin = rabbit_data_coercion:to_binary(Node),
    ThisNodeSize = size(term_to_binary(node())) + 1,
    Pid = spawn(fun () -> ok end),
    %% drop the local node data from a local pid
    <<_:ThisNodeSize/binary, LocalPidData/binary>> = term_to_binary(Pid),
    S = size(NodeBin),
    %% replace it with the incoming node binary
    Final = <<131,103, 100, S:16/unsigned, NodeBin/binary, LocalPidData/binary>>,
    binary_to_term(Final).
