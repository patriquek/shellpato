/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Messaging;

import Database.Configurations;
import java.util.Properties;
import javax.ejb.Asynchronous;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.json.JSONObject;

/**
 *
 * @author manmaish
 */
@Asynchronous
public class QueueWriter {

    private String queue;
    private final String JMS_FACTORY = new Configurations().getConfig("JMSConnectionFactory");
    private JSONObject objmsg = new JSONObject();
    private Queue ESBQueue;
    private TextMessage msg;

    public QueueWriter(String queue, JSONObject objmsg) {
        this.queue = queue;
        this.objmsg = objmsg;
    }

    /**
     *
     * @return @throws NamingException
     * @throws JMSException
     */
    @Asynchronous
    public boolean sendObjectToQueue()
            throws NamingException, JMSException {
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;

        Properties p = new Properties();

        p.put("java.naming.provider.url", new Configurations().getConfig("PROVIDER_URL"));

        Context cont = new InitialContext(p);
        try {
            ConnectionFactory conFact = (ConnectionFactory) cont.lookup(this.JMS_FACTORY);

            connection = conFact.createConnection();

            session = connection.createSession(false, 1);

            ESBQueue = (Queue) cont.lookup(this.queue);

            producer = session.createProducer(ESBQueue);
            this.msg = session.createTextMessage(this.objmsg.toString());
            producer.send(this.msg);
            return true;
        } catch (Exception ex) {

            System.err.println("QueueWriter Error sendObjectToQueue()" + ex.getMessage());
            return false;
        } finally {
            if (connection != null) {
                connection.close();
            }
            if (session != null) {
                session.close();
            }
            if (producer != null) {
                producer.close();
            }
        }
    }
}
