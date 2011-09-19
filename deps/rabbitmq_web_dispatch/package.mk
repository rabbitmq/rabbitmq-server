RELEASABLE:=true
DEPS:=mochiweb-wrapper webmachine-wrapper
WITH_BROKER_TEST_COMMANDS:=rabbit_mochiweb_test:test()
STANDALONE_TEST_COMMANDS:=rabbit_mochiweb_test_unit:test()
