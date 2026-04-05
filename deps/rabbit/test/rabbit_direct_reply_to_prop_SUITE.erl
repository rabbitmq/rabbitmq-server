-module(rabbit_direct_reply_to_prop_SUITE).

-compile(export_all).

-include_lib("proper/include/proper.hrl").

-define(ITERATIONS_TO_RUN_UNTIL_CONFIDENT, 10000).

all() ->
    [
     decode_reply_to,
     volatile_queue_detection
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


decode_reply_to(Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun() -> prop_decode_reply_to(Config) end,
      [],
      ?ITERATIONS_TO_RUN_UNTIL_CONFIDENT).

volatile_queue_detection(Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun() -> prop_volatile_queue_detection(Config) end,
      [],
      ?ITERATIONS_TO_RUN_UNTIL_CONFIDENT).

prop_volatile_queue_detection(_) ->
    ?FORALL(Suffix, non_empty(binary()),
        begin
            VolatileName = <<"amq.rabbitmq.reply-to.", Suffix/binary>>,
            rabbit_volatile_queue:is(VolatileName)
            andalso not rabbit_volatile_queue:is(<<"notvolatile.", Suffix/binary>>)
            andalso not rabbit_volatile_queue:is(<<"amq.rabbitmq.reply-to">>)
            andalso not rabbit_volatile_queue:is(<<"amq.rabbitmq.reply-t.", Suffix/binary>>)
        end).

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
            IxPartsEnc = base64:encode(rabbit_pid_codec:recompose_to_binary(IxParts)),
            QNameBin = <<"amq.rabbitmq.reply-to.", IxPartsEnc/binary, ".", Key/binary>>,

            NodeMap   = maps:from_list(NodeList),
            NoNodeMap = maps:from_list(NoNodeList),

            %% There is non-zero chance Random is a valid encoded Pid.
            NonB64 = <<0, Random/binary>>, 

            {ok, rabbit_pid_codec:recompose(PidParts)} =:=
                rabbit_volatile_queue:pid_from_name(QNameBin, NodeMap)
            andalso error =:=
                rabbit_volatile_queue:pid_from_name(QNameBin, NoNodeMap)
            andalso error =:=
                rabbit_volatile_queue:pid_from_name(NonB64, NodeMap)
        end).
