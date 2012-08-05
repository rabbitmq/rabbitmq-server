# RabbitMQ MQTT adapter

## Supports

* QoS0 and QoS1 publish & consume
* Last Will (LWT)
* SSL
* Session stickiness

## AMQP mapping

             (MQTT)    ADAPTER    (AMQP)

*    QOS0 flow publish

             PUBLISH   ------>    basic.publish

*    QOS0 flow receive

             PUBLISH   <------    basic.deliver

*    QOS1 flow publish (tracked in state.unacked_pub)

             PUBLISH   ------>    basic.publish  --+
                                                   |
             PUBACK    <------    basic.ack     <--+

*   QOS1 flow receive (tracked in state.awaiting_ack)

        +--  PUBLISH   <------    basic.deliver
        |
        +--> PUBACK    ------>    basic.ack

