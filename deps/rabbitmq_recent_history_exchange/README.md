# RabbitMQ Recent History Cache

Keeps track of the last 20 messages that passed thorugh the exchange. Everytime a queue is bound to the exchange it delivers that last 20 messages to them. This is usefull for implementing a very simple __Chat History__ where clients that join the conversation can get the latest messages.

Exchange Type: `x-recent-history`

## Installation     
    
    git clone git://github.com/videlalvaro/rabbitmq-recent-history-exchange.git
    cd rabbitmq-recent-history-exchange
    make package
    cp dist/*.ez $RABBITMQ_HOME/plugins

## License 

See LICENSE.md
