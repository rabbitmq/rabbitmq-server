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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(ets_compat).

%% Those functions are exported for internal use only, not for public
%% consumption.
-export([update_counter/4,
         update_counter_pre_18/4,
         update_counter_post_18/4]).

-export([update_element/3,
         update_element_pre_18/3,
         update_element_post_18/3]).

-erlang_version_support([
                         {18, [
                               {update_counter, 4,
                                update_counter_pre_18,
                                update_counter_post_18},
                               {update_element, 3,
                                update_element_pre_18,
                                update_element_post_18}
                              ]}
                        ]).

%% ets:update_counter(Tab, Key, Incr, Default) appeared in Erlang 18.x.
%% We need a wrapper for Erlang R16B03 and Erlang 17.x.

update_counter(Tab, Key, Incr, Default) ->
    code_version:update(?MODULE),
    ?MODULE:update_counter(Tab, Key, Incr, Default).

update_counter_pre_18(Tab, Key, Incr, Default) ->
    %% The wrapper tries to update the counter first. If it's missing
    %% (and a `badarg` is raised), it inserts the default value and
    %% tries to update the counter one more time.
    try
        ets:update_counter(Tab, Key, Incr)
    catch
        _:badarg ->
            try
                %% There is no atomicity here, so between the
                %% call to `ets:insert_new/2` and the call to
                %% `ets:update_counter/3`, the the counters have
                %% a temporary value (which is not possible with
                %% `ets:update_counter/4). Furthermore, there is a
                %% chance for the counter to be removed between those
                %% two calls as well.
                ets:insert_new(Tab, Default),
                ets:update_counter(Tab, Key, Incr)
            catch
                _:badarg ->
                    %% We can't tell with just `badarg` what the real
                    %% cause is. We have no way to decide if we should
                    %% try to insert/update the counter again, so let's
                    %% do nothing.
                    0
            end
    end.

update_counter_post_18(Tab, Key, Incr, Default) ->
    ets:update_counter(Tab, Key, Incr, Default).

%% ets:update_element(Tab, Key, ElementSpec) appeared in Erlang 18.x.
%% We need a wrapper for Erlang R16B03 and Erlang 17.x.

update_element(Tab, Key, ElementSpec) ->
    code_version:update(?MODULE),
    ?MODULE:update_element(Tab, Key, ElementSpec).

update_element_pre_18(Tab, Key, {Pos, Value}) ->
    case ets:lookup(Tab, Key) of
        [] ->
            ok;
        [Tuple] ->
            ets:insert(Tab, setelement(Pos, Tuple, Value))
    end.

update_element_post_18(Tab, Key, ElementSpec) ->
    ets:update_element(Tab, Key, ElementSpec).
