
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class QueueEvents {
    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        ConnectionFactory f = new ConnectionFactory();
        Connection c = f.newConnection();
        Channel ch = c.createChannel();
        String q = ch.queueDeclare().getQueue();
        ch.queueBind(q, "amq.rabbitmq.event", "queue.*");

        Consumer consumer = new DefaultConsumer(ch) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String event = envelope.getRoutingKey();
                Map<String, Object> headers = properties.getHeaders();
                String name = headers.get("name").toString();
                String vhost = headers.get("vhost").toString();

                if (event.equals("queue.created")) {
                    boolean durable = (Boolean) headers.get("durable");
                    String durableString = durable ? " (durable)" : " (transient)";
                    System.out.println("Created: " + name + " in " + vhost + durableString);
                }
                else /* queue.deleted is the only other possibility */ {
                    System.out.println("Deleted: " + name + " in " + vhost);
                }
            }
        };
        ch.basicConsume(q, true, consumer);
        System.out.println("QUEUE EVENTS");
        System.out.println("============\n");
    }
}
