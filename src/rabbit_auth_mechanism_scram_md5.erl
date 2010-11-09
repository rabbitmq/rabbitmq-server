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

-module(rabbit_auth_mechanism_scram_md5).
-include("rabbit.hrl").

-behaviour(rabbit_auth_mechanism).

-export([description/0, should_offer/1, init/0, handle_response/2]).

-include("rabbit_auth_mechanism_spec.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "auth mechanism plain"},
                    {mfa,         {rabbit_registry, register,
                                   [auth_mechanism, <<"RABBIT-SCRAM-MD5">>,
                                    ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

-record(state, {username = undefined, salt2 = undefined}).

%% START-OK: Username
%% SECURE: {Salt1, Salt2} (where Salt1 is the salt from the db and
%% Salt2 differs every time)
%% SECURE-OK: md5(Salt2 ++ md5(Salt1 ++ Password))

%% The second salt is there to defend against replay attacks. The
%% first is needed since the passwords are salted in the db.

%% This is only somewhat improved security over PLAIN (if you can
%% break MD5 you can still replay attack) but it's better than nothing
%% and mostly there to prove the use of SECURE / SECURE-OK frames.

description() ->
    [{name, <<"RABBIT-SCRAM-MD5">>},
     {description, <<"RabbitMQ SCRAM-MD5 authentication mechanism">>}].

should_offer(_Sock) ->
    true.

init() ->
    #state{}.

handle_response(Username, State = #state{username = undefined}) ->
    case rabbit_access_control:lookup_user(Username) of
        {ok, User} ->
            <<Salt1:4/binary, _/binary>> = User#user.password_hash,
            Salt2 = rabbit_access_control:make_salt(),
            {challenge, <<Salt1/binary, Salt2/binary>>,
             State#state{username = Username, salt2 = Salt2}};
        {error, not_found} ->
            {refused, Username} %% TODO information leak
    end;

handle_response(Response, #state{username = Username, salt2 = Salt2}) ->
    case rabbit_access_control:lookup_user(Username) of
        {ok, User} ->
            <<_:4/binary, Hash/binary>> = User#user.password_hash,
            Expected = erlang:md5(<<Salt2/binary, Hash/binary>>),
            case Response of
                Expected -> {ok, User};
                _        -> {refused, Username}
            end;
        {error, not_found} ->
            {refused, Username}
    end.
