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
%%  The Initial Developer of the Original Code is GoPivotal, Inc.
%%  Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_federation_status).
-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

-export([start_link/0]).

-export([report/4, remove_exchange_or_queue/1, remove/2, status/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-import(rabbit_federation_util, [name/1]).

-define(SERVER, ?MODULE).
-define(ETS_NAME, ?MODULE).

-record(state, {}).
-record(entry, {key, uri, status, timestamp}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

report(Upstream, UParams, XorQName, Status) ->
    gen_server:cast(?SERVER, {report, Upstream, UParams, XorQName, Status,
                              calendar:local_time()}).

remove_exchange_or_queue(XorQName) ->
    gen_server:call(?SERVER, {remove_exchange_or_queue, XorQName}, infinity).

remove(Upstream, XorQName) ->
    gen_server:call(?SERVER, {remove, Upstream, XorQName}, infinity).

status() ->
    gen_server:call(?SERVER, status, infinity).

init([]) ->
    ?ETS_NAME = ets:new(?ETS_NAME,
                        [named_table, {keypos, #entry.key}, private]),
    {ok, #state{}}.

handle_call({remove_exchange_or_queue, XorQName}, _From, State) ->
    [link_gone(Entry)
     || Entry <- ets:match_object(?ETS_NAME, match_entry(xorqkey(XorQName)))],
    {reply, ok, State};

handle_call({remove, Upstream, XorQName}, _From, State) ->
    case ets:match_object(?ETS_NAME, match_entry(key(XorQName, Upstream))) of
        [Entry] -> link_gone(Entry);
        []      -> ok
    end,
    {reply, ok, State};

handle_call(status, _From, State) ->
    Entries = ets:tab2list(?ETS_NAME),
    {reply, [format(Entry) || Entry <- Entries], State}.

handle_cast({report, Upstream, #upstream_params{safe_uri = URI},
             XorQName, Status, Timestamp}, State) ->
    Entry = #entry{key        = key(XorQName, Upstream),
                   status     = Status,
                   uri        = URI,
                   timestamp  = Timestamp},
    true = ets:insert(?ETS_NAME, Entry),
    rabbit_event:notify(federation_link_status, format(Entry)),
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format(#entry{status    = Status,
              uri       = URI,
              timestamp = Timestamp} = Entry) ->
    identity(Entry) ++ split_status(Status) ++ [{uri,       URI},
                                                {timestamp, Timestamp}].

identity(#entry{key       = {#resource{virtual_host = VHost,
                                       kind         = Type,
                                       name         = XorQNameBin},
                             UpstreamName, UXorQNameBin}}) ->
    case Type of
        exchange -> [{exchange,          XorQNameBin},
                     {upstream_exchange, UXorQNameBin}];
        queue    -> [{queue,             XorQNameBin},
                     {upstream_queue,    UXorQNameBin}]
    end ++ [{type,      Type},
            {vhost,     VHost},
            {upstream,  UpstreamName}].

split_status({running, ConnName})         -> [{status,           running},
                                              {local_connection, ConnName}];
split_status({Status, Error})             -> [{status, Status},
                                              {error,  Error}];
split_status(Status) when is_atom(Status) -> [{status, Status}].

link_gone(Entry) ->
    rabbit_event:notify(federation_link_removed, identity(Entry)),
    true = ets:delete_object(?ETS_NAME, Entry).

%% We don't want to key off the entire upstream, bits of it may change
key(XName = #resource{kind = exchange}, #upstream{name          = UpstreamName,
                                                  exchange_name = UXNameBin}) ->
    {XName, UpstreamName, UXNameBin};

key(QName = #resource{kind = queue}, #upstream{name       = UpstreamName,
                                               queue_name = UQNameBin}) ->
    {QName, UpstreamName, UQNameBin}.

xorqkey(XorQName) ->
    {XorQName, '_', '_'}.

match_entry(Key) ->
    #entry{key               = Key,
           uri               = '_',
           status            = '_',
           timestamp         = '_'}.
