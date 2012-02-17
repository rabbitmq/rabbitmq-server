-module(rabbit_ws_sup).

-export([start_link/0, start_processor/1]).

-define(SUP_NAME, rabbit_ws_client_top_sup).

%%----------------------------------------------------------------------------

start_link() ->
    rabbit_client_sup:start_link({local, ?SUP_NAME},
                                 {rabbit_ws_client_sup, start_link, []}).

start_processor(Params) ->
    supervisor:start_child(?SUP_NAME, [Params]).
