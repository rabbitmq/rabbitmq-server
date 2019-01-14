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
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

%% Legacy hashing implementation, only used as a last resort when
%% #internal_user.hashing_algorithm is md5 or undefined (the case in
%% pre-3.6.0 user records).

-module(rabbit_password_hashing_md5).

-behaviour(rabbit_password_hashing).

-export([hash/1]).

hash(Binary) ->
    erlang:md5(Binary).
