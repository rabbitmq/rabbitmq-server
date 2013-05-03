%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ Federation.
%%
%%  The Initial Developer of the Original Code is VMware, Inc.
%%  Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_status).
-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

-export([start_link/0]).

-export([report/4, remove_exchange/1, remove/2, status/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-import(rabbit_federation_util, [name/1]).

-define(SERVER, ?MODULE).
-define(ETS_NAME, ?MODULE).

-record(state, {}).
-record(entry, {key, uri, status, timestamp}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

report(Upstream, UParams, XName, Status) ->
    gen_server:cast(?SERVER, {report, Upstream, UParams, XName, Status,
                              calendar:local_time()}).

remove_exchange(XName) ->
    gen_server:call(?SERVER, {remove_exchange, XName}, infinity).

remove(Upstream, XName) ->
    gen_server:call(?SERVER, {remove, Upstream, XName}, infinity).

status() ->
    gen_server:call(?SERVER, status, infinity).

init([]) ->
    ?ETS_NAME = ets:new(?ETS_NAME,
                        [named_table, {keypos, #entry.key}, private]),
    {ok, #state{}}.

handle_call({remove_exchange, XName}, _From, State) ->
    true = ets:match_delete(?ETS_NAME, match_entry(xkey(XName))),
    {reply, ok, State};

handle_call({remove, Upstream, XName}, _From, State) ->
    true = ets:match_delete(?ETS_NAME, match_entry(key(XName, Upstream))),
    {reply, ok, State};

handle_call(status, _From, State) ->
    Entries = ets:tab2list(?ETS_NAME),
    {reply, [format(Entry) || Entry <- Entries], State}.

handle_cast({report, Upstream, #upstream_params{uri = URI0},
             XName, Status, Timestamp}, State) ->
    URI = rabbit_federation_upstream:remove_credentials(URI0),
    true = ets:insert(?ETS_NAME,
                      #entry{key        = key(XName, Upstream),
                             status     = Status,
                             uri        = URI,
                             timestamp  = Timestamp}),
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format(#entry{key       = {#resource{virtual_host = VHost,
                                     kind         = exchange,
                                     name         = XNameBin},
                           Connection, UXNameBin},
              status    = Status,
              uri       = URI,
              timestamp = Timestamp}) ->
        [{exchange,          XNameBin},
         {vhost,             VHost},
         {connection,        Connection},
         {uri,               URI},
         {upstream_exchange, UXNameBin},
         {status,            Status},
         {timestamp,         Timestamp}].

%% We don't want to key off the entire upstream, bits of it may change
key(XName, #upstream{name          = UpstreamName,
                     exchange_name = UXNameBin}) ->
    {XName, UpstreamName, UXNameBin}.

xkey(XName) ->
    {XName, '_', '_'}.

match_entry(Key) ->
    #entry{key               = Key,
           uri               = '_',
           status            = '_',
           timestamp         = '_'}.
