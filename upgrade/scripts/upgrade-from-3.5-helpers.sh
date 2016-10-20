setup_steps() {
    local test_vhost='test'
    local test_user='test_user'

    ${RABBITMQCTL} add_user $test_user $test_user
    ${RABBITMQCTL} set_user_tags $test_user policymaker
    ${RABBITMQCTL} add_vhost $test_vhost
    ${RABBITMQCTL} set_permissions -p $test_vhost $test_user '.*' '.*' '.*'
    ${RABBITMQCTL} set_policy -p $test_vhost my_policy_name "policy.*" '{"max-length":300}'

    # TODO: create exchanges, queues, publish messages
    local exchange_name="test_exchange"
    local queue_name_base="test_queue"

    prepare_rabbitmqadmin

    ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost declare exchange name=$exchange_name type=fanout
    for i in `seq 1 $QUEUES_COUNT_TRANSIENT`
    do
        local queue_name="${queue_name_base}_trans_${i}"
        ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost declare queue name=$queue_name durable=false
        ./rabbitmqadmin -utest_user -ptest_user -V $test_vhost declare binding source=$exchange_name destination=$queue_name
    done

    for i in `seq 1 $QUEUES_COUNT_DURABLE`
    do
        local queue_name="${queue_name_base}_dur_${i}"
        ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost declare queue name=$queue_name durable=true
        ./rabbitmqadmin -utest_user -ptest_user -V $test_vhost declare binding source=$exchange_name destination=$queue_name
    done

    local sequence_index=`seq 1 $INDEX_MSG_SIZE`
    local msg_payload_index=`printf '=%.0s' $sequence_index`
    local sequence_store=`seq 1 $STORE_MSG_SIZE`
    local msg_payload_store=`printf '+%.0s' $sequence_store`
    for i in `seq 1 $MSGS_COUNT_NON_PERSISTENT`
    do
        ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost publish routing_key=any exchange=$exchange_name payload=$msg_payload
    done

    for i in `seq 1 $MSGS_COUNT_PERSISTENT_INDEX`
    do
        ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost publish routing_key=any exchange=$exchange_name payload=$msg_payload_index properties='{"delivery_mode":2}'
    done

    for i in `seq 1 $MSGS_COUNT_PERSISTENT_STORE`
    do
        ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost publish routing_key=any exchange=$exchange_name payload=$msg_payload_store properties='{"delivery_mode":2}'
    done

    # Second vhost to test data isolation

    local test_vhost1="test_vhost_1"
    local msg_payload_index_1=`printf '_%.0s' $sequence_index`
    local msg_payload_store_1=`printf '0%.0s' $sequence_store`

    ${RABBITMQCTL} add_vhost $test_vhost1
    ${RABBITMQCTL} set_permissions -p $test_vhost1 $test_user '.*' '.*' '.*'

    ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost1 declare exchange name=$exchange_name type=fanout
    for i in `seq 1 $QUEUES_COUNT_DURABLE`
    do
        local queue_name="${queue_name_base}_dur_vhost1_${i}"
        ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost1 declare queue name=$queue_name durable=true
        ./rabbitmqadmin -utest_user -ptest_user -V $test_vhost1 declare binding source=$exchange_name destination=$queue_name
    done
    for i in `seq 1 $MSGS_COUNT_PERSISTENT_INDEX`
    do
        ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost1 publish routing_key=any exchange=$exchange_name payload=$msg_payload_index_1 properties='{"delivery_mode":2}'
    done

    for i in `seq 1 $MSGS_COUNT_PERSISTENT_STORE`
    do
        ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost1 publish routing_key=any exchange=$exchange_name payload=$msg_payload_store_1 properties='{"delivery_mode":2}'
    done

    ./rabbitmqadmin -utest_user -ptest_user -V $test_vhost list queues name durable messages
    ./rabbitmqadmin -utest_user -ptest_user -V $test_vhost list exchanges
}

