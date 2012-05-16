%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(amqp_client_SUITE).

-export([test_coverage/0]).

-include_lib("eunit/include/eunit.hrl").

-define(FUNCTION,
        begin
            catch throw(x),
            Fun = case erlang:get_stacktrace() of
                      [{_, F, _}    | _] -> F; %% < R15
                      [{_, F, _, _} | _] -> F %% >= R15
                  end,
            list_to_atom(string:strip(atom_to_list(Fun), right, $_))
        end).

-define(RUN(Props), run(?FUNCTION, Props)).

%%---------------------------------------------------------------------------
%% Tests
%%---------------------------------------------------------------------------

amqp_uri_parse_test_()                  -> ?RUN([]).
basic_get_test_()                       -> ?RUN([]).
basic_get_ipv6_test_()                  -> ?RUN([]).
basic_return_test_()                    -> ?RUN([]).
simultaneous_close_test_()              -> ?RUN([repeat]).
basic_qos_test_()                       -> ?RUN([]).
basic_recover_test_()                   -> ?RUN([]).
basic_consume_test_()                   -> ?RUN([]).
consume_notification_test_()            -> ?RUN([]).
basic_nack_test_()                      -> ?RUN([]).
large_content_test_()                   -> ?RUN([]).
lifecycle_test_()                       -> ?RUN([]).
nowait_exchange_declare_test_()         -> ?RUN([]).
channel_repeat_open_close_test_()       -> ?RUN([]).
channel_multi_open_close_test_()        -> ?RUN([]).
basic_ack_test_()                       -> ?RUN([]).
basic_ack_call_test_()                  -> ?RUN([]).
channel_lifecycle_test_()               -> ?RUN([]).
queue_unbind_test_()                    -> ?RUN([]).
sync_method_serialization_test_()       -> ?RUN([]).
async_sync_method_serialization_test_() -> ?RUN([]).
sync_async_method_serialization_test_() -> ?RUN([]).
teardown_test_()                        -> ?RUN([repeat]).
rpc_test_()                             -> ?RUN([]).
pub_and_close_test_()                   -> ?RUN([]).
channel_tune_negotiation_test_()        -> ?RUN([]).
confirm_test_()                         -> ?RUN([]).
confirm_barrier_test_()                 -> ?RUN([]).
confirm_barrier_nop_test_()             -> ?RUN([]).
confirm_barrier_timeout_test_()         -> ?RUN([]).
confirm_barrier_die_timeout_test_()     -> ?RUN([]).
default_consumer_test()                 -> ?RUN([]).
subscribe_nowait_test_()                -> ?RUN([]).

non_existent_exchange_test_()           -> ?RUN([negative]).
bogus_rpc_test_()                    -> ?RUN([negative, repeat]).
hard_error_test_()                   -> ?RUN([negative, repeat]).
non_existent_user_test_()               -> ?RUN([negative]).
invalid_password_test_()                -> ?RUN([negative]).
non_existent_vhost_test_()              -> ?RUN([negative]).
no_permission_test_()                   -> ?RUN([negative]).
channel_writer_death_test_()            -> ?RUN([negative]).
channel_death_test_()                   -> ?RUN([negative]).
shortstr_overflow_property_test_()      -> ?RUN([negative]).
shortstr_overflow_field_test_()         -> ?RUN([negative]).
command_invalid_over_channel_test_()    -> ?RUN([negative]).
command_invalid_over_channel0_test_()   -> ?RUN([negative]).

%%---------------------------------------------------------------------------
%% Internal
%%---------------------------------------------------------------------------

run(TestName, Props) ->
    RepeatCount = case proplists:get_value(repeat, Props, false) of
                      true                          -> 100;
                      Number when is_number(Number) -> Number;
                      false                         -> 1
                  end,
    Module = case proplists:get_bool(negative, Props) of
                 true  -> negative_test_util;
                 false -> test_util
             end,
    {timeout, proplists:get_value(timeout, Props, 60),
     fun () ->
             lists:foreach(
                 fun (_) ->
                         try erlang:apply(Module, TestName, []) of
                             Ret -> Ret
                         catch
                             exit:normal -> ok
                         end
                 end, lists:seq(1, RepeatCount))
     end}.

%%---------------------------------------------------------------------------
%% Coverage
%%---------------------------------------------------------------------------

test_coverage() ->
    rabbit_misc:enable_cover(),
    test(),
    rabbit_misc:report_cover().

