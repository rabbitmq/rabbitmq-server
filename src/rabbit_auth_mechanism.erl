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
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_auth_mechanism).

-ifdef(use_specs).

%% A description.
-callback description() -> [proplists:property()].

%% If this mechanism is enabled, should it be offered for a given socket?
%% (primarily so EXTERNAL can be SSL-only)
-callback should_offer(rabbit_net:socket()) -> boolean().

%% Called before authentication starts. Should create a state
%% object to be passed through all the stages of authentication.
-callback init(rabbit_net:socket()) -> any().

%% Handle a stage of authentication. Possible responses:
%% {ok, User}
%%     Authentication succeeded, and here's the user record.
%% {challenge, Challenge, NextState}
%%     Another round is needed. Here's the state I want next time.
%% {protocol_error, Msg, Args}
%%     Client got the protocol wrong. Log and die.
%% {refused, Msg, Args}
%%     Client failed authentication. Log and die.
-callback handle_response(binary(), any()) ->
    {'ok', rabbit_types:user()} |
    {'challenge', binary(), any()} |
    {'protocol_error', string(), [any()]} |
    {'refused', string(), [any()]}.

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{description, 0}, {should_offer, 1}, {init, 1}, {handle_response, 2}];
behaviour_info(_Other) ->
    undefined.

-endif.
