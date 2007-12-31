package com.rabbitmq.management;

import com.caucho.hessian.io.*;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.RpcClient;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class ProxyFactory {

    static Logger log = Logger.getLogger(ProxyFactory.class);

    private static final int TIME_OUT = 2000;

    public static Object createProxy(final Class clazz, final RpcClient client) {        

        InvocationHandler handler = new InvocationHandler() {
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                //log.info("Calling method -> " + method.getName());
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                Hessian2Output output = new Hessian2Output(os);
                output.call(method.getName(), args);

                output.flush();
                AMQP.BasicProperties properties = new AMQP.BasicProperties();
                properties.contentType = "application/x-hessian";
                byte[] result = client.primitiveCall(properties, os.toByteArray(), TIME_OUT);


                ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
                HessianDebugInputStream dis = new HessianDebugInputStream(is, new PrintWriter(System.err));

                int ch;
                while ((ch = dis.read()) >= 0) {}

                byte[] b2 = os.toByteArray();
                for (byte b : b2) {
                    System.err.print((b & 0xff) + ",");
                }

                System.err.println();
                System.err.println("-------------");

                if (null == result) {
                    throw new RuntimeException("RPC call did not yield a result");
                }


                Hessian2Input input = new Hessian2Input(new ByteArrayInputStream(result));

                Object response = null;
                
                try {
                    input.startReply();
                    response = input.readObject();
                    input.completeReply();
                }
                catch (HessianServiceException e) {
                    log.error(e);
                    throw new RuntimeException(e.getMessage());                    
                }


                return response;
            }
        };

        return Proxy.newProxyInstance(clazz.getClassLoader(), new Class[] {clazz}, handler);
    }


}
