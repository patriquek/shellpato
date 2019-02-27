/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Messaging;

import Database.Configurations;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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

/**
 *
 * @author manmaish
 */
@Asynchronous
public class LogWriter {

    private final String queue;//queue string
    private TextMessage msg;
    private final String JMS_FACTORY = new Configurations().getConfig("JMSConnectionFactory");
    private String logMessage;
    private Queue ESBRequestQueue;
    private String timestamp;
    private final String directory = "DCBalanceInquiry";

    private final DateTimeFormatter formatter;
    private ZonedDateTime zonedDateTime;

    public LogWriter(String logMessage) {
        this.formatter = DateTimeFormatter.ofPattern("dd-MMM-yyyy HH:mm:ss.SSS");
        this.queue = new Configurations().getConfig("MAIN_LOG_QUEUE");
        this.logMessage = logMessage;
    }

    /**
     * Processes log structure
     *
     * @param lFile
     */
    @Asynchronous
    public void log(String lFile) {
        try {

            zonedDateTime = ZonedDateTime.now();
            timestamp = formatter.format(zonedDateTime);
            this.logMessage = ("###" + directory + "$$$" + lFile + "###" + timestamp + "::" + this.logMessage);
            sendObjectToQueue();
        } catch (NamingException | JMSException ex) {
            System.err.println("LogWriter Error log()" + ex.getMessage());
        }
    }

    @Asynchronous
    public void logMessage(String lFile) {
        try {
            zonedDateTime = ZonedDateTime.now();
            timestamp = formatter.format(zonedDateTime);
            this.logMessage = ("###" + directory + "$$$" + lFile + "###" + timestamp + "::" + this.logMessage);
            sendObjectToQueue();
        } catch (NamingException | JMSException ex) {
            System.err.println("LogWriter Error logMessage()" + ex.getMessage());
        }
    }

    /**
     * Writes log message to the log queue
     *
     * @return
     * @throws NamingException
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
            System.err.println("ErrorMessage" + this.logMessage);
            connection = conFact.createConnection();

            session = connection.createSession(false, 1);

            ESBRequestQueue = (Queue) cont.lookup(this.queue);

            producer = session.createProducer(ESBRequestQueue);
            this.msg = session.createTextMessage(this.logMessage);
            producer.send(this.msg);

            return true;
        } catch (Exception ex) {
            System.err.println("LogWriter Error sendObjectToQueue()" + ex.getMessage());
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
