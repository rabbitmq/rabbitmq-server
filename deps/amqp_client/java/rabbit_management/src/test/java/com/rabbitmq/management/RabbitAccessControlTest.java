package com.rabbitmq.management;

import com.rabbitmq.client.*;
import junit.framework.TestCase;
import org.apache.log4j.Logger;

import java.util.List;

public class RabbitAccessControlTest extends TestCase {

    static Logger log = Logger.getLogger(RabbitAccessControlTest.class);

    private Channel channel;
    private int ticket;

    private String userName = "guest";
    private String password = "guest";
    private String virtualHost = "/";
    private String hostName = "localhost";
    private String realm = "/data";
    private int portNumber = 5672;
    private String exchangeName = "x";
    private String routingKey = "a.b.c.d";
    private Connection conn;
    private RabbitAccessControl rabbitAccessControl;

    private String testUsername = "foo";
    private String testPassword = "bar";


    protected void setUp() throws Exception {
        ConnectionParameters params = new ConnectionParameters();
        params.setUsername(userName);
        params.setPassword(password);
        params.setVirtualHost(virtualHost);
        params.setRequestedHeartbeat(0);
        ConnectionFactory factory = new ConnectionFactory(params);
        conn = factory.newConnection(hostName, portNumber);
        channel = conn.createChannel();
        ticket = channel.accessRequest(realm);
        RpcClient rpc = new RpcClient(channel, ticket, exchangeName, routingKey);
        rabbitAccessControl = (RabbitAccessControl) ProxyFactory.createProxy(RabbitAccessControl.class, rpc);

        List users = rabbitAccessControl.list_users();

        if (users.contains(testUsername)) {
            rabbitAccessControl.delete_user(testUsername);
        }

        users = rabbitAccessControl.list_users();

        assertFalse("New user (" + testUsername + ") not deleted", users.contains(testUsername));
    }

    protected void tearDown() throws Exception {
        channel.close(AMQP.REPLY_SUCCESS, "Goodbye");
        conn.close(AMQP.REPLY_SUCCESS, "Goodbye");
    }

    public void testAddDeleteUser() throws Exception {
        try {
            rabbitAccessControl.delete_user(testUsername);
            fail("New user (" + testUsername + ") should not have been deleted");
        }
        catch (Exception e) {
            assertEquals("no_such_user", e.getMessage());

        }
        rabbitAccessControl.add_user(testUsername, testPassword);
        List users = rabbitAccessControl.list_users();
        assertTrue("New user (" + testUsername + ") not added", users.contains(testUsername));
        rabbitAccessControl.delete_user(testUsername);
        users = rabbitAccessControl.list_users();
        assertFalse("New user (" + testUsername + ") not deleted", users.contains(testUsername));
    }

    public void testChangePassword() {
        rabbitAccessControl.add_user(testUsername, testPassword);        
        rabbitAccessControl.change_password(testUsername, testPassword + "99");
    }

    public void testLookup() {
        rabbitAccessControl.add_user(testUsername, testPassword);
        User user = rabbitAccessControl.lookup_user(testUsername);
        assertNotNull(user);
        assertEquals(testPassword, user.getPassword());
    }
}
    
