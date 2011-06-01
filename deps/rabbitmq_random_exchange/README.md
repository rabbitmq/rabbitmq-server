# RabbitMQ Random Exchange Type

This exchange type is for load-balancing among consumers. It's basically 
a direct exchange, with the exception that, instead of each consumer bound 
to that exchange with the same routing key getting a copy of the message, 
the exchange type randomly selects a queue to route to.

There is no weighting or anything, so maybe load "balancing" might be a bit 
of a misnomer. It uses Erlang's crypto:rand_uniform/2 function, if you're 
interested.

### Installation

    git clone git://github.com/jbrisbin/random-exchange.git
    cd random-exchange
    make package
    cp dist/*.ez $RABBITMQ_HOME/plugins

### Usage

To use it, declare an exchange of type "x-random".

    Apache 2.0 Licensed:
    http://www.apache.org/licenses/LICENSE-2.0.html