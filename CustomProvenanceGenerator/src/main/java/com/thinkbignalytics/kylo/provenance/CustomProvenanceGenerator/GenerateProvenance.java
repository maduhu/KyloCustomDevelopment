package com.thinkbignalytics.kylo.provenance.CustomProvenanceGenerator;

/**
 * @author Shashi Vishwakarma
 * 
 */

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.jms.JMSException;
import org.joda.time.DateTime;

import com.thinkbiganalytics.nifi.provenance.jms.ProvenanceEventActiveMqWriter;
import com.thinkbiganalytics.nifi.provenance.model.ProvenanceEventRecordDTO;
import com.thinkbiganalytics.nifi.provenance.model.ProvenanceEventRecordDTOHolder;


public class GenerateProvenance {

	public static void main (String args[])

	{

		GenerateProvenance generateEvent = new GenerateProvenance();
		List<ProvenanceEventRecordDTO> eventPool = new  ArrayList(10);

		System.out.println("Constructing Event ");
		eventPool.add(generateEvent.constructEvent());

		//Create Provenance Holder 
		System.out.println("Passing to Provenance Holder");
		ProvenanceEventRecordDTOHolder provenanceHolder = new ProvenanceEventRecordDTOHolder();
		provenanceHolder.setEvents(eventPool);

		//Publish Event to Activemq to be consumed by Kylo
		System.out.println("Publish event to Activemq");
		generateEvent.publishEventToKylo(provenanceHolder);

	}


	/**
	 * 
	 * @return provenance event object
	 */
	public ProvenanceEventRecordDTO constructEvent()
	{

		//Job is used for tracking for each flow file ID
		UUID provenanceJobId = UUID.randomUUID();

		//Unique event attached with each flowfile ID.
		long eventId =0;

		//Random batch ID for processing
		UUID batchId = UUID.randomUUID();

		//Feedname created in Kylo application.
		String feedName = "toy_stores.products";

		// Processor Group - Random UUID
		UUID feedProcessorGroup = UUID.randomUUID();

		//Component Type
		String componentType = "Processor";

		//Event Date 
		DateTime eventDateTime = new DateTime(1498913928);
		DateTime previousEventTime = new DateTime(1498913957);

		Long eventDuration = (long) 30;
		UUID idthree = UUID.randomUUID();
		ProvenanceEventRecordDTO newProv = new ProvenanceEventRecordDTO();
		newProv.setComponentId(idthree.toString());
		newProv.setComponentName("Spark Streaming Event " + eventId);
		newProv.setJobFlowFileId(provenanceJobId.toString());
		newProv.setFlowFileUuid(provenanceJobId.toString());
		newProv.setEventId(eventId +100);
		newProv.setBatchId(batchId.toString());
		newProv.setFeedName(feedName);
		newProv.setFeedProcessGroupId(feedProcessorGroup.toString());
		newProv.setIsEndOfJob(false);
		newProv.setComponentType(componentType);
		newProv.setIsBatchJob(true);
		newProv.setIsStartOfJob(false);
		newProv.setEventType("ATTRIBUTES_MODIFIED");
		newProv.setEventTime(eventDateTime);
		newProv.setStartTime(previousEventTime);
		newProv.setEventDuration(eventDuration);

		return newProv;

	}

	/**
	 * 
	 * @param provenanceHolder Provenance Event Holder 
	 */
	public void publishEventToKylo(ProvenanceEventRecordDTOHolder provenanceHolder)
	{

		ActivemqWriterKylo activemqWriter = new ActivemqWriterKylo();
		try {
			System.out.println("About to Publish Event");
			activemqWriter.publishMessageToKylo(provenanceHolder);
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Failed to publish message to activemq " +e.getMessage());

		}

	}


}
