prepare_rabbitmqadmin() {
    rm -rf rabbitmqadmin
    wget localhost:15672/cli/rabbitmqadmin
    chmod +x rabbitmqadmin
}