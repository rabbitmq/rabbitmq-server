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

-module(rabbit_auth_backend).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
     %% A description proplist as with auth mechanisms,
     %% exchanges. Currently unused.
     {description, 0},

     %% Check a user can log in, given a username and a proplist of
     %% authentication information (e.g. [{password, Password}]).
     %%
     %% Possible responses:
     %% {ok, User}
     %%     Authentication succeeded, and here's the user record.
     %% {error, Msg, Args}
     %%     Something went wrong. Log and die.
     %% {refused, Msg, Args}
     %%     Client failed authentication. Log and die.
     {check_user_login, 2},

     %% Given #user, vhost path and permission, can a user access a vhost?
     %% Permission is read  - learn of the existence of (only relevant for
     %%                       management plugin)
     %%            or write - log in
     %%
     %% Possible responses:
     %% true
     %% false
     %% {error, Msg}
     %%     Something went wrong. Log and die.
     {check_vhost_access, 3},

     %% Given #user, resource and permission, can a user access a resource?
     %%
     %% Possible responses:
     %% true
     %% false
     %% {error, Msg}
     %%     Something went wrong. Log and die.
     {check_resource_access, 3}
    ];
behaviour_info(_Other) ->
    undefined.
