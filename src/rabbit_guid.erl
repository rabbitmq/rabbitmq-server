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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_guid).

-include("rabbit.hrl").

-behaviour(gen_server).

-export([start_link/0]).
-export([guid/0, string_guid/1, binstring_guid/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {serial}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> {'ok', pid()} | 'ignore' | {'error', any()}).
-spec(guid/0 :: () -> guid()).
-spec(string_guid/1 :: (any()) -> string()).
-spec(binstring_guid/1 :: (any()) -> binary()).

-endif.

%%----------------------------------------------------------------------------

start_link() ->
    %% The persister can get heavily loaded, and we don't want that to
    %% impact guid generation.  We therefore keep the serial in a
    %% separate process rather than calling rabbit_persister:serial/0
    %% directly in the functions below.
    gen_server:start_link({local, ?SERVER}, ?MODULE,
                          [rabbit_persister:serial()], []).

%% generate a guid that is monotonically increasing per process.
%%
%% The id is only unique within a single cluster and as long as the
%% persistent message store hasn't been deleted.
guid() ->
    %% We don't use erlang:now() here because a) it may return
    %% duplicates when the system clock has been rewound prior to a
    %% restart, or ids were generated at a high rate (which causes
    %% now() to move ahead of the system time), and b) it is really
    %% slow since it takes a global lock and makes a system call.
    %%
    %% rabbit_persister:serial/0, in combination with self/0 (which
    %% includes the node name) uniquely identifies a process in space
    %% and time. We combine that with a process-local counter to give
    %% us a GUID that is monotonically increasing per process.
    G = case get(guid) of
            undefined -> {{gen_server:call(?SERVER, serial, infinity), self()},
                          0};
            {S, I}   -> {S, I+1}
        end,
    put(guid, G),
    G.

%% generate a readable string representation of a guid. Note that any
%% monotonicity of the guid is not preserved in the encoding.
string_guid(Prefix) ->
    %% we use the (undocumented) ssl_base64 module here because it is
    %% present throughout OTP R11 and R12 whereas base64 only becomes
    %% available in R11B-4.
    %%
    %% TODO: once debian stable and EPEL have moved from R11B-2 to
    %% R11B-4 or later we should change this to use base64.
    Prefix ++ "-" ++ ssl_base64:encode(erlang:md5(term_to_binary(guid()))).

binstring_guid(Prefix) ->
    list_to_binary(string_guid(Prefix)).

%%----------------------------------------------------------------------------

init([Serial]) ->
    {ok, #state{serial = Serial}}.

handle_call(serial, _From, State = #state{serial = Serial}) ->
    {reply, Serial, State};

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
