package com.rabbitmq.management;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import com.caucho.hessian.io.HessianInput;
import com.caucho.hessian.io.HessianServiceException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.RpcClient;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class ProxyFactory {

    static Logger log = Logger.getLogger(ProxyFactory.class);

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
                byte[] result = client.primitiveCall(properties, os.toByteArray());

                //log.info("Result size : " + result.length);

//                System.err.println("-------------------");
//                for(byte b : result) {
//                    System.err.print((int)b + ",");
//                }
//                System.err.println("");

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
