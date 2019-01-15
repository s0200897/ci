package com.go.jms;

import com.ibm.mq.jms.*;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.TextMessage;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static javax.jms.Session.AUTO_ACKNOWLEDGE;

public class MQMain {

    public static final String ESB_MESSAGES_DIR = "D:/tmp/ET/output";

    public static void main(String[] args) throws JMSException, IOException {
//        uatPutMessageToTwoQueues();

        String content = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<esb>\n" +
                "    <Header>\n" +
                "    </Header>\n" +
                "    <Body>\n" +
                "    </Body>\n" +
                "</esb>";

        testAccountInquiry(content);
    }

    private static void uatPutMessageToTwoQueues() throws JMSException, MalformedURLException {
        final String MQ_SERVER_QUEUE_MANAGER = "*ESBQM1_SVR,*ESBQM2_SVR";
        final String MQ_SERVER_CHANNEL = "USER.HSTSVR.SVRCONN";
        final String MQ_SERVER_QUEUE_OUT = "Q.AbC.SERVER.OUT";
        final String MQ_CCDT_URL = "file:/etc/CCDT_UAT_AbC.TAB";
        final String MQ_SERVER_HOSTS = "10.30.201.101,10.30.201.102";
        final String MQ_SERVER_PORT = "1415";
        final String MQ_SERVER_QUEUE_IN = "Q.AbC.SERVER.IN";

        System.out.println("(1) Initializing connection factory, session and queue...");
        List<MQQueueConnectionFactory> mqQueueConnectionFactoryList = new ArrayList<>();
        List<MQConnection> mqConnectionList = new ArrayList<>();
        List<MQSession> mqSessionList = new ArrayList<>();
        List<Queue> requestQueueList = new ArrayList<>();
        List<MQMessageProducer> producerList = new ArrayList<>();

        int i = 1;
        for (String queueManager : MQ_SERVER_QUEUE_MANAGER.split(",")) {
            MQQueueConnectionFactory factory = new MQQueueConnectionFactory();
            try {
                // factory.setCCDTURL(new URL(MQ_CCDT_URL));
                factory.setQueueManager(queueManager);
                factory.setHostName("localhost"); // local run with aid host port forwarded.
                factory.setPort(14150 + i);
                factory.setChannel(MQ_SERVER_CHANNEL);
                factory.setCCSID(1821);// ET
                factory.setTransportType(1);// ET
                mqQueueConnectionFactoryList.add(factory);

                MQConnection connection = (MQConnection) factory.createConnection();
                mqConnectionList.add(connection);
                connection.start();

                MQSession session = (MQSession) connection.createSession(false, AUTO_ACKNOWLEDGE);
                mqSessionList.add(session);

                Queue queue = session.createQueue(MQ_SERVER_QUEUE_OUT);
                requestQueueList.add(queue);

                MQMessageProducer producer = (MQMessageProducer) session.createProducer(queue);
                producerList.add(producer);

                i++;
            } catch (Exception e) {
                e.printStackTrace();
            }

            // System.err.printf("Queue Manage %s connection factory initialized.%s %n", queueManager, factory);
        }

        System.out.println("(2) Put message to queue evenly...");
        final AtomicInteger counter = new AtomicInteger();
        try {
            Files.newDirectoryStream(
                    Paths.get(ESB_MESSAGES_DIR),
                    path -> path.toString().endsWith(".xml"))
                    .forEach(xml -> {
                        int index = counter.incrementAndGet();
                        MQSession session = mqSessionList.get(index % 2);
                        MQMessageProducer producer = producerList.get(index % 2);
                        System.out.printf("%03d.[QUEUE-%d] %s\n", index, index % 2, xml);

                        try {
                            // put94aMsgToQueue(serverMQQueueConnectionFactory, MSG_AbC_94a);
                            String msg94a = new String(Files.readAllBytes(xml));
                            TextMessage message = session.createTextMessage();
                            message.setText(msg94a);
                            producer.send(message);
                            // System.out.println(msg94a);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (AutoCloseable resource : producerList) closing(resource);
        for (AutoCloseable resource : mqSessionList) closing(resource);
        for (AutoCloseable resource : mqConnectionList) closing(resource);
    }

    private static void closing(AutoCloseable resource) {

        try {
            if (resource != null) resource.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (resource != null) {
                try {
                    resource.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static MQQueueConnectionFactory createMqQueueConnectionFactory
            (String host, int port, String queueManager, String channel) throws JMSException {

        MQQueueConnectionFactory mqQueueConnectionFactory = new MQQueueConnectionFactory();
        // mqQueueConnectionFactory.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
        mqQueueConnectionFactory.setHostName(host);
        mqQueueConnectionFactory.setPort(port);
        mqQueueConnectionFactory.setQueueManager(queueManager);
        mqQueueConnectionFactory.setChannel(channel);
        mqQueueConnectionFactory.setCCSID(1821);// ET
        mqQueueConnectionFactory.setTransportType(1);// ET

        return mqQueueConnectionFactory;
    }

    private static void testAccountInquiry(String content) throws JMSException {

        final String HOST = "localhost";
        final int PORT = 1414;
        final String CHANNEL = "USER.HOST.SVRCONN";
        final String QMGR = "*ESBQM";
        final String HK_QUEUE_IN = "Q.AbC.IN.SHFB";
        final String HK_QUEUE_OUT = "Q.AbC.OUT.SHFB";
        final String SG_QUEUE_IN = "Q.AbC.IN";
        final String SG_QUEUE_OUT = "Q.AbC.OUT";
        MQQueueConnectionFactory factory = createMqQueueConnectionFactory(HOST, PORT, QMGR, CHANNEL);

        MQConnection mqConnection = (MQConnection) factory.createConnection();
        mqConnection.start();
        MQSession mqSession = (MQSession) mqConnection.createSession(false, AUTO_ACKNOWLEDGE);
        Queue requestQueue = mqSession.createQueue(HK_QUEUE_IN);
        Queue replyQueue = mqSession.createQueue(HK_QUEUE_OUT);
        MQMessageProducer producer = (MQMessageProducer) mqSession.createProducer(requestQueue);

        TextMessage message = mqSession.createTextMessage();
        message.setText(content);
        message.setJMSReplyTo(replyQueue);

        producer.send(message);

        // receive in sync
        String msgId = message.getJMSMessageID();
        MQMessageConsumer consumer = (MQMessageConsumer)
                mqSession.createConsumer(replyQueue, "JMSCorrelationID='" + msgId + "'");

        System.out.printf("collecting message with id: %s, from queue:%s\n", msgId, replyQueue);
        Message replyMessage = consumer.receive(12300);
        String replyBody = ((TextMessage) replyMessage).getText();
        System.out.println(replyBody);
        System.out.println(replyMessage);

        consumer.close();
        producer.close();
        mqSession.close();
        mqConnection.close();
    }
}
