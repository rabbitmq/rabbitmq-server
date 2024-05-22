-module(rabbit_direct_reply_to_prop_SUITE).

-compile(export_all).

-include_lib("proper/include/proper.hrl").

-define(ITERATIONS_TO_RUN_UNTIL_CONFIDENT, 10000).

all() ->
    [
     decode_reply_to_v2
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%% Tests %%%


decode_reply_to_v2(Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun() -> prop_decode_reply_to(Config) end,
      [],
      ?ITERATIONS_TO_RUN_UNTIL_CONFIDENT).

prop_decode_reply_to(_) ->
    ?FORALL({Len, Random}, {pos_integer(), binary()},
        begin
            Key      = <<"apple">>,
            NodeList = lists:map(
                         fun(I) -> {I, list_to_atom(integer_to_list(I))} end,
                         lists:seq(1, Len)
                        ),

            [ {Ix, Node} | NoNodeList ] = NodeList,

            PidParts   = #{node => Node, id => 0, serial => 0, creation => 0},
            IxParts    = PidParts#{node := rabbit_nodes_common:make("banana", Ix)},
            IxPartsEnc = base64:encode(pid_recomposition:to_binary(IxParts)),
            IxBin      = <<IxPartsEnc/binary, ".", Key/binary>>,

            NodeMap   = maps:from_list(NodeList),
            NoNodeMap = maps:from_list(NoNodeList),

            %% There is non-zero chance Random is a valid encoded Pid.
            NonB64 = <<0, Random/binary>>, 

            {ok, pid_recomposition:recompose(PidParts), Key} =:=
                rabbit_direct_reply_to:decode_reply_to_v2(IxBin, NodeMap)
            andalso {error, target_node_not_found} =:=
                rabbit_direct_reply_to:decode_reply_to_v2(IxBin, NoNodeMap)
            andalso {error, unrecognized_format} =:=
                rabbit_direct_reply_to:decode_reply_to_v2(NonB64, NodeMap)
        end).
