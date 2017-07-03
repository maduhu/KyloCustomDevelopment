package com.thinkbignalytics.kylo.provenance.CustomProvenanceGenerator;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.jms.core.MessageCreator;

import com.thinkbiganalytics.nifi.provenance.model.ProvenanceEventRecordDTOHolder;

public class ActivemqWriterKylo
{
	private static String url = "tcp://127.0.0.1:61616";

	private static String subject = "thinkbig.feed-manager"; 

	public void publishMessageToKylo(ProvenanceEventRecordDTOHolder provenanceHolder) throws JMSException {

		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
		Connection connection = connectionFactory.createConnection();
		connection.start();

		Session session = connection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);
		Destination destination = session.createQueue(subject);
		MessageProducer producer = session.createProducer(destination);

		//Create meesage object before sending to Queue
		ObjectMapperSerializerKylo objectSerializer = new ObjectMapperSerializerKylo();
		ObjectMessage message = session.createObjectMessage(objectSerializer.serialize(provenanceHolder));

		producer.send(message);
		System.out.println("Sentage '" + message.toString() + "'");

		connection.close();
	}
}