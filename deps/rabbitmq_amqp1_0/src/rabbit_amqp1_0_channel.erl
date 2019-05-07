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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_amqp1_0_channel).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

-export([call/2, call/3, cast/2, cast/3, cast_flow/3, subscribe/3]).
-export([convert_code/1, convert_error/1]).

-import(rabbit_amqp1_0_util, [protocol_error/3]).

call(Ch, Method) ->
    convert_error(fun () -> amqp_channel:call(Ch, Method) end).

call(Ch, Method, Content) ->
    convert_error(fun () -> amqp_channel:call(Ch, Method, Content) end).

cast(Ch, Method) ->
    convert_error(fun () -> amqp_channel:cast(Ch, Method) end).

cast(Ch, Method, Content) ->
    convert_error(fun () -> amqp_channel:cast(Ch, Method, Content) end).

cast_flow(Ch, Method, Content) ->
    convert_error(fun () -> amqp_channel:cast_flow(Ch, Method, Content) end).

subscribe(Ch, Method, Subscriber) ->
    convert_error(fun () -> amqp_channel:subscribe(Ch, Method, Subscriber) end).

convert_error(Fun) ->
    try
        Fun()
        catch exit:{{shutdown, {server_initiated_close, Code, Msg}}, _} ->
            protocol_error(convert_code(Code), Msg, [])
    end.

%% TODO this was completely off the top of my head. Check these make sense.
convert_code(?CONTENT_TOO_LARGE)   -> ?V_1_0_AMQP_ERROR_FRAME_SIZE_TOO_SMALL;
convert_code(?NO_ROUTE)            -> ?V_1_0_AMQP_ERROR_PRECONDITION_FAILED;
convert_code(?NO_CONSUMERS)        -> ?V_1_0_AMQP_ERROR_PRECONDITION_FAILED;
convert_code(?ACCESS_REFUSED)      -> ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS;
convert_code(?NOT_FOUND)           -> ?V_1_0_AMQP_ERROR_NOT_FOUND;
convert_code(?RESOURCE_LOCKED)     -> ?V_1_0_AMQP_ERROR_RESOURCE_LOCKED;
convert_code(?PRECONDITION_FAILED) -> ?V_1_0_AMQP_ERROR_PRECONDITION_FAILED;
convert_code(?CONNECTION_FORCED)   -> ?V_1_0_CONNECTION_ERROR_CONNECTION_FORCED;
convert_code(?INVALID_PATH)        -> ?V_1_0_AMQP_ERROR_INVALID_FIELD;
convert_code(?FRAME_ERROR)         -> ?V_1_0_CONNECTION_ERROR_FRAMING_ERROR;
convert_code(?SYNTAX_ERROR)        -> ?V_1_0_CONNECTION_ERROR_FRAMING_ERROR;
convert_code(?COMMAND_INVALID)     -> ?V_1_0_CONNECTION_ERROR_FRAMING_ERROR;
convert_code(?CHANNEL_ERROR)       -> ?V_1_0_CONNECTION_ERROR_FRAMING_ERROR;
convert_code(?UNEXPECTED_FRAME)    -> ?V_1_0_CONNECTION_ERROR_FRAMING_ERROR;
convert_code(?RESOURCE_ERROR)      -> ?V_1_0_AMQP_ERROR_RESOURCE_LIMIT_EXCEEDED;
convert_code(?NOT_ALLOWED)         -> ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS;
convert_code(?NOT_IMPLEMENTED)     -> ?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED;
convert_code(?INTERNAL_ERROR)      -> ?V_1_0_AMQP_ERROR_INTERNAL_ERROR.
