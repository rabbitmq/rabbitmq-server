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

-module(rabbit_msg_store_index).

-export([behaviour_info/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(behaviour_info/1 ::
	(_) ->
			       'undefined' |
			       [{'delete' |
				 'delete_by_file' |
				 'insert' |
				 'lookup' |
				 'new' |
				 'recover' |
				 'terminate' |
				 'update' |
				 'update_fields',
				 1 | 2 | 3},
				...]).

-endif.

%%----------------------------------------------------------------------------

behaviour_info(callbacks) ->
    [{new,            1},
     {recover,        1},
     {lookup,         2},
     {insert,         2},
     {update,         2},
     {update_fields,  3},
     {delete,         2},
     {delete_by_file, 2},
     {terminate,      1}];
behaviour_info(_Other) ->
    undefined.
