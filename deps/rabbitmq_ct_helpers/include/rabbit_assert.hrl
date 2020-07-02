-define(awaitMatch(Guard, Expr, Timeout),
        begin
            ((fun Filter(Horizon) ->
                      R = Expr,
                      case (R) of
                          Guard -> R;
                          __V -> case erlang:system_time(millisecond) of
                                     Now when Now < Horizon ->
                                         timer:sleep(min(50, Horizon - Now)),
                                         Filter(Horizon);
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
