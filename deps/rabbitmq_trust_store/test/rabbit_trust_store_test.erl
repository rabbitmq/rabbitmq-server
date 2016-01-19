-module(rabbit_trust_store_test).
-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").


%% ...

library_test() ->

    %% Given: Makefile.

    {Root, Certificate, PublicKey} = ct_helper:make_certs().
