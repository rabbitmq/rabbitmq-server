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
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_db).

-export([init/0]).

-export([get_active_suffix/3, set_active_suffix/3]).

%% TODO get rid of this dets table, use mnesia
-define(DETS_NAME, "rabbit_federation_exchange.dat").

init() ->
    F = file(),
    Args = [{type, set}],
    case dets:open_file(F, Args) of
        {ok, F} -> ok;
        Error   -> rabbit_log:error("Federation state file ~s could "
                                    "not be opened - ~p.", [F, Error]),
                   ok = file:delete(F),
                   {ok, F} = dets:open_file(F, Args)
    end.

%%----------------------------------------------------------------------------

get_active_suffix(X, Upstream, Default) ->
    case dets:lookup(file(), {suffix, X, Upstream}) of
        []       -> Default;
        [{_, S}] -> S
    end.

set_active_suffix(X, Upstream, Suffix) ->
    ok = dets:insert(file(), {{suffix, X, Upstream}, Suffix}).

%%----------------------------------------------------------------------------

file() -> rabbit_mnesia:dir() ++ "/" ++ ?DETS_NAME.
