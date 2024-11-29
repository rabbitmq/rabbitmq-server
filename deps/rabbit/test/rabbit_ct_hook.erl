-module(rabbit_ct_hook).

-export([init/2]).

init(_, _) ->
    _ = rabbit_ct_helpers:redirect_logger_to_ct_logs([]),
	{ok, undefined}.
