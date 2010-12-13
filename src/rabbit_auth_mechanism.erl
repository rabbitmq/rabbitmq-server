%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_auth_mechanism).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
     %% A description.
     {description, 0},

     %% Called before authentication starts. Should create a state
     %% object to be passed through all the stages of authentication.
     {init, 1},

     %% Handle a stage of authentication. Possible responses:
     %% {ok, User}
     %%     Authentication succeeded, and here's the user record.
     %% {challenge, Challenge, NextState}
     %%     Another round is needed. Here's the state I want next time.
     %% {protocol_error, Msg, Args}
     %%     Client got the protocol wrong. Log and die.
     %% {refused, Username}
     %%     Client failed authentication. Log and die.
     {handle_response, 2}
    ];
behaviour_info(_Other) ->
    undefined.
