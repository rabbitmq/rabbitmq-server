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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_consumer).

-export([info_all/1]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(info_all/1 :: (rabbit_types:vhost()) -> [rabbit_types:infos()]).

-endif.

%%----------------------------------------------------------------------------

info_all(VHostPath) ->
    [[{queue_name,   QName#resource.name},
      {channel_pid,  ChPid},
      {consumer_tag, ConsumerTag},
      {ack_required, AckRequired}] ||
     #amqqueue{pid=QPid, name=QName} <- rabbit_amqqueue:list(VHostPath),
     {ChPid, ConsumerTag, AckRequired} <-
          delegate:invoke(QPid, fun (P) ->
                                    gen_server2:call(P, consumers, infinity)
                                end)].
