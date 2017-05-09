-include_lib("eunit/include/eunit.hrl").

-define(OK, 200).
-define(CREATED, 201).
-define(NO_CONTENT, 204).
-define(SEE_OTHER, 303).
-define(BAD_REQUEST, 400).
-define(NOT_AUTHORISED, 401).
%%-define(NOT_FOUND, 404). Defined for AMQP by amqp_client.hrl (as 404)
%% httpc seems to get racy when using HTTP 1.1
-define(HTTPC_OPTS, [{version, "HTTP/1.0"}, {autoredirect, false}]).
