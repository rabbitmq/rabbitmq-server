%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2010-2014 GoPivotal, Inc.  All rights reserved.
%%

%% We need to ensure all responses are application/json; anything
%% coming back as text/html could constitute an XSS vector. Also I'm
%% sure it's easier on our clients if they can always expect JSON
%% responses.
%%
%% Based on webmachine_error_handler, but I'm not sure enough remains
%% to be copyrightable.

-module(rabbit_webmachine_error_handler).

-export([render_error/3]).

render_error(Code, Req, Reason) ->
    case Req:has_response_body() of
        {true, _}  -> maybe_log(Req, Reason),
                      Req:response_body();
        {false, _} -> render_error_body(Code, Req:trim_state(), Reason)
    end.

render_error_body(404,  Req, Reason) -> error_body(404,  Req, "Not Found");
render_error_body(Code, Req, Reason) -> error_body(Code, Req, Reason).

error_body(Code, Req, Reason) ->
    {ok, ReqState} = Req:add_response_header("Content-Type","application/json"),
    case Code of
        500 -> maybe_log(Req, Reason);
        _   -> ok
    end,
    Json = {struct,
            [{error,  list_to_binary(httpd_util:reason_phrase(Code))},
             {reason, list_to_binary(rabbit_misc:format("~p~n", [Reason]))}]},
    {mochijson2:encode(Json), ReqState}.

maybe_log(_Req, {error, {exit, normal, _Stack}}) ->
    %% webmachine_request did an exit(normal), so suppress this
    %% message. This usually happens when a chunked upload is
    %% interrupted by network failure.
    ok;
maybe_log(Req, Reason) ->
    {Path, _} = Req:path(),
    error_logger:error_msg("webmachine error: path=~p~n~p~n", [Path, Reason]).
