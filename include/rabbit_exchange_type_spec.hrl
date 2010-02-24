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
-ifdef(use_specs).

-spec(description/0 :: () -> [{atom(), any()}]).
-spec(publish/2 :: (exchange(), delivery()) -> {routing_result(), [pid()]}).
-spec(validate/1 :: (exchange()) -> 'ok').
-spec(create/1 :: (exchange()) -> 'ok').
-spec(recover/2 :: (exchange(), list(binding())) -> 'ok').
-spec(delete/2 :: (exchange(), list(binding())) -> 'ok').
-spec(add_binding/2 :: (exchange(), binding()) -> 'ok').
-spec(remove_bindings/2 :: (exchange(), list(binding())) -> 'ok').

-endif.
