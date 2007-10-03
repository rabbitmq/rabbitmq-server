EBIN_DIR=ebin
SOURCE_DIR=src
TEST_SOURCE_DIR=test
INCLUDE_DIR=include
ERLC_FLAGS=-W0

compile:
	mkdir -p $(EBIN_DIR)
	erlc -I $(INCLUDE_DIR) -o $(EBIN_DIR) $(ERLC_FLAGS) $(SOURCE_DIR)/*.erl
	erlc -I $(INCLUDE_DIR) -o $(EBIN_DIR) $(ERLC_FLAGS) $(TEST_SOURCE_DIR)/*.erl

all: compile

clean:
	rm $(EBIN_DIR)/*.beam
