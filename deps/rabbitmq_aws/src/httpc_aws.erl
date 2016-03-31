-module(httpc_aws).

%% API exports
-export([]).

%% Export all for unit tests
-ifdef(TEST).
-compile(export_all).
-endif.

-define(ALGORITHM, "AWS4-HMAC-SHA256").
-define(MIME_AWS_JSON, "application/x-amz-json-1.0").
-define(SCHEME, https).


%%====================================================================
%% API functions
%%====================================================================


%%====================================================================
%% Internal functions
%%====================================================================
