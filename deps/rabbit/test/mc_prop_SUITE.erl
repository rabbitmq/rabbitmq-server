-module(mc_prop_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("proper/include/proper.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbit/include/mc.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [shuffle], all_tests()}
    ].

all_tests() ->
    [
     amqpl_amqp_convert_never_crashes
    ].

%%%===================================================================
%%% Test cases
%%%===================================================================

amqpl_amqp_convert_never_crashes(_Config) ->
    Property = fun() -> prop_amqpl_amqp_convert_never_crashes() end,
    rabbit_ct_proper_helpers:run_proper(Property, [], 1000).

prop_amqpl_amqp_convert_never_crashes() ->
    Anns = annotations(),
    ?FORALL(
       {PropsBin, AppPropsBin, MsgAnnBin, FooterBin, BodyBin, UseAmqpType},
       {binary(), binary(), binary(), binary(), binary(), boolean()},
       begin
           Headers0 = [{<<"x-amqp-1.0-properties">>, longstr, PropsBin},
                       {<<"x-amqp-1.0-app-properties">>, longstr, AppPropsBin},
                       {<<"x-amqp-1.0-message-annotations">>, longstr, MsgAnnBin},
                       {<<"x-amqp-1.0-footer">>, longstr, FooterBin}],
           Type = case UseAmqpType of
                      true -> <<"amqp-1.0">>;
                      false -> undefined
                  end,
           Props = #'P_basic'{headers = Headers0,
                              type = Type,
                              delivery_mode = 2},
           Content = #content{properties = Props,
                              payload_fragments_rev = [BodyBin]},
           Msg = mc:init(mc_amqpl, Content, Anns),
           try mc:convert(mc_amqp, Msg) of
               Result -> Result =/= undefined
           catch
               _:_ -> false
           end
       end).

%% Utility

annotations() ->
    #{?ANN_EXCHANGE => <<"exch">>,
      ?ANN_ROUTING_KEYS => [<<"apple">>]}.
