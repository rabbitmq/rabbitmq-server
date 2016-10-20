verify_steps() {
    local test_vhost='test'
    local test_user='test_user'

    ${RABBITMQCTL} list_users | grep $test_user
    ${RABBITMQCTL} list_vhosts | grep $test_vhost
    ${RABBITMQCTL} list_user_permissions $test_user | grep $test_vhost | grep -F ".*"
    ${RABBITMQCTL} list_permissions -p $test_vhost | grep $test_user | grep -F ".*"
    ${RABBITMQCTL} list_policies -p $test_vhost | \
        grep $test_vhost | \
        grep my_policy_name | \
        grep -F "policy.*" | \
        grep -F '{"max-length":300}'

    local exchange_name="test_exchange"
    local queue_name_base="test_queue"

    prepare_rabbitmqadmin
    ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost list exchanges | grep $exchange_name

    local sequence_index=`seq 1 $INDEX_MSG_SIZE`
    local msg_payload_index=`printf '=%.0s' $sequence_index`
    local sequence_store=`seq 1 $STORE_MSG_SIZE`
    local msg_payload_store=`printf '+%.0s' $sequence_store`
    
    # Durable queues survive upgrade
    for i in `seq 1 $QUEUES_COUNT_DURABLE`
    do
        local queue_name="${queue_name_base}_dur_${i}"
        ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost list queues name durable | \
            grep $queue_name | grep True
        # Each queue have $MSGS_COUNT_PERSISTENT_INDEX + $MSGS_COUNT_PERSISTENT_STORE messages
        ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost list queues name messages | \
            grep $queue_name | grep `expr $MSGS_COUNT_PERSISTENT_INDEX + $MSGS_COUNT_PERSISTENT_STORE`

        # Drain persistent messages from queue index
        for j in `seq 1 $MSGS_COUNT_PERSISTENT_INDEX`
        do
            ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost get queue=$queue_name count=1 ackmode=ack_requeue_false | \
                grep $msg_payload_index | grep $INDEX_MSG_SIZE | grep $exchange_name
        done
        # Drain persistent messages from message store
        for j in `seq 1 $MSGS_COUNT_PERSISTENT_STORE`
        do
            ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost get queue=$queue_name count=1 ackmode=ack_requeue_false | \
                grep $msg_payload_store | grep $STORE_MSG_SIZE | grep $exchange_name
        done
        # No more messages
        ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost get queue=$queue_name count=1 ackmode=ack_requeue_false | \
            grep "No items"

    done

    # Transient queues are deleted
    (! ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost list queues name | grep "${queue_name_base}_trans_") || exit 2

    # Test first vhost and check second
    ${RABBITMQCTL} delete_vhost $test_vhost

    # Also delete default vhost
    ${RABBITMQCTL} delete_vhost /

    local test_vhost1="test_vhost_1"
    local msg_payload_index_1=`printf '_%.0s' $sequence_index`
    local msg_payload_store_1=`printf '0%.0s' $sequence_store`

    ${RABBITMQCTL} list_vhosts | grep $test_vhost1
    ${RABBITMQCTL} list_user_permissions $test_user | grep $test_vhost1 | grep -F ".*"
    ${RABBITMQCTL} list_permissions -p $test_vhost1 | grep $test_user | grep -F ".*"

    ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost1 list exchanges | grep $exchange_name

    # Durable queues
    for i in `seq 1 $QUEUES_COUNT_DURABLE`
    do
        local queue_name="${queue_name_base}_dur_vhost1_${i}"
        ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost1 list queues name durable | \
            grep $queue_name | grep True
        # Each queue have $MSGS_COUNT_PERSISTENT_INDEX + $MSGS_COUNT_PERSISTENT_STORE messages
        ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost1 list queues name messages | \
            grep $queue_name | grep `expr $MSGS_COUNT_PERSISTENT_INDEX + $MSGS_COUNT_PERSISTENT_STORE`

        # Drain persistent messages from queue index
        for j in `seq 1 $MSGS_COUNT_PERSISTENT_INDEX`
        do
            ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost1 get queue=$queue_name count=1 ackmode=ack_requeue_false | \
                grep $msg_payload_index_1 | grep $INDEX_MSG_SIZE | grep $exchange_name
        done
        # Drain persistent messages from message store
        for j in `seq 1 $MSGS_COUNT_PERSISTENT_STORE`
        do
            ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost1 get queue=$queue_name count=1 ackmode=ack_requeue_false | \
                grep $msg_payload_store_1 | grep $STORE_MSG_SIZE | grep $exchange_name
        done
        # No more messages
        ./rabbitmqadmin -u $test_user -p $test_user -V $test_vhost1 get queue=$queue_name count=1 ackmode=ack_requeue_false | \
            grep "No items"
    done

    echo "Finish verify"
}


ackmode=ack_requeue_false