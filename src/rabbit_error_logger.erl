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

-module(rabbit_error_logger).
-include("rabbit.hrl").

-define(LOG_EXCH_NAME, <<"amq.rabbitmq.log">>).

-behaviour(gen_event).

-export([init/1, terminate/2, code_change/3, handle_call/2, handle_event/2, handle_info/2]).

init([DefaultVHost]) ->
    #exchange{} = rabbit_exchange:declare(
                    rabbit_misc:r(DefaultVHost, exchange, ?LOG_EXCH_NAME),
                    topic, true, false, []),
    {ok, #resource{virtual_host = DefaultVHost,
                   kind = exchange,
                   name = ?LOG_EXCH_NAME}}.

terminate(_Arg, _State) ->
    terminated_ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_event({Kind, _Gleader, {_Pid, Format, Data}}, State) ->
    ok = publish(Kind, Format, Data, State),
    {ok, State};
handle_event(_Event, State) ->
    {ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

publish(error, Format, Data, State) ->
    publish1(<<"error">>, Format, Data, State);
publish(warning_msg, Format, Data, State) ->
    publish1(<<"warning">>, Format, Data, State);
publish(info_msg, Format, Data, State) ->
    publish1(<<"info">>, Format, Data, State);
publish(_Other, _Format, _Data, _State) ->
    ok.

publish1(RoutingKey, Format, Data, LogExch) ->
    {ok, _QueueNames} = rabbit_exchange:simple_publish(
                          false, false, LogExch, RoutingKey, <<"text/plain">>,
                          list_to_binary(io_lib:format(Format, Data))),
    ok.
