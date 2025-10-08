-define(AWAIT_MATCH_DEFAULT_POLLING_INTERVAL, 50).

-define(awaitMatch(Guard, Expr, Timeout, PollingInterval),
        begin
            ((fun AwaitMatchFilter(AwaitMatchHorizon) ->
                      AwaitMatchResult = Expr,
                      case (AwaitMatchResult) of
                          Guard -> AwaitMatchResult;
                          __V -> case erlang:monotonic_time(millisecond) of
                                     AwaitMatchNow when AwaitMatchNow < AwaitMatchHorizon ->
                                         timer:sleep(
                                           min(PollingInterval,
                                               AwaitMatchHorizon - AwaitMatchNow)),
                                         AwaitMatchFilter(AwaitMatchHorizon);
                                     _ ->
                                         erlang:error({awaitMatch,
                                                       [{module, ?MODULE},
                                                        {line, ?LINE},
                                                        {expression, (??Expr)},
                                                        {pattern, (??Guard)},
                                                        {value, __V},
                                                        {timeout, Timeout},
                                                        {polling_interval, PollingInterval}]})
                                 end
                      end
              end)(erlang:monotonic_time(millisecond) + Timeout))
        end).

-define(awaitMatch(Guard, Expr, Timeout),
        ?awaitMatch(Guard, Expr, Timeout, ?AWAIT_MATCH_DEFAULT_POLLING_INTERVAL)).
