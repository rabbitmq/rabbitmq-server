-define(awaitMatch(Guard, Expr, Timeout),
        begin
            ((fun AwaitMatchFilter(AwaitMatchHorizon) ->
                      AwaitMatchResult = Expr,
                      case (AwaitMatchResult) of
                          Guard -> AwaitMatchResult;
                          __V -> case erlang:system_time(millisecond) of
                                     AwaitMatchNow when AwaitMatchNow < AwaitMatchHorizon ->
                                         timer:sleep(min(50, AwaitMatchHorizon - AwaitMatchNow)),
                                         AwaitMatchFilter(AwaitMatchHorizon);
                                     _ ->
                                         erlang:error({awaitMatch,
                                                       [{module, ?MODULE},
                                                        {line, ?LINE},
                                                        {expression, (??Expr)},
                                                        {pattern, (??Guard)},
                                                        {value, __V}]})
                                 end
                      end
              end)(erlang:system_time(millisecond) + Timeout))
        end).
