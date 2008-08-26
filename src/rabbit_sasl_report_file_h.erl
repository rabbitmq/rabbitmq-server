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
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial Technologies
%%   LLC., and Rabbit Technologies Ltd. are Copyright (C) 2007-2008
%%   LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_sasl_report_file_h).

-behaviour(gen_event).

-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

init({File, []}) ->
    sasl_report_file_h:init({File, sasl_error_logger_type()});
init({_File, _Type} = FileInfo) ->
    sasl_report_file_h:init(FileInfo);
init(File) ->
    sasl_report_file_h:init({File, sasl_error_logger_type()}).

handle_event(Event, State) ->
    sasl_report_file_h:handle_event(Event, State).

handle_info(Event, State) ->
    sasl_report_file_h:handle_info(Event, State).

handle_call(Event, State) ->
    sasl_report_file_h:handle_call(Event, State).

terminate(Reason, State) ->
    sasl_report_file_h:terminate(Reason, State).

code_change(OldVsn, State, Extra) ->
    sasl_report_file_h:code_change(OldVsn, State, Extra).

sasl_error_logger_type() ->
    case application:get_env(sasl, errlog_type) of
        {ok, error}    -> error;
        {ok, progress} -> progress;
        {ok, all}      -> all;
        {ok, Bad}      -> throw({error, {wrong_errlog_type, Bad}});
        _              -> all
    end.
