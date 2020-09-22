package maersk.com.mq.metrics.mqmetrics;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.xml.namespace.QName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import com.ibm.mq.MQException;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.pcf.PCFMessageAgent;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.MeterRegistry;
import maersk.com.mq.events.MQConnection;
import maersk.com.mq.events.MQEvents;
import maersk.com.mq.events.MQMetricsQueueManager;
import maersk.com.mq.json.entities.Metric;

//@ActiveProfiles("test")
//@SpringBootApplication

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { MQEvents.class })
@Component
@ActiveProfiles("test")
public class MQEventTests {

	private final static Logger log = LoggerFactory.getLogger(MQEventTests.class);
		
	@Autowired
	private MQMetricsQueueManager qman;
	public MQMetricsQueueManager getQueMan() {
		return this.qman;
	}
	
	@Autowired
	private MQConnection conn;
	
	@Autowired
	private MeterRegistry meterRegistry;
	
	@Value("${ibm.mq.queueManager}")
	private String queueManager;
	public void setQueueManager(String v) {
		this.queueManager = v;
	}
	public String getQueueManagerName() { return this.queueManager; }
	

	@Test
	@Order(1)
	public void findGaugeMetrics() throws MQDataException, ParseException, InterruptedException {
		
		log.info("Attempting to connect to {}", getQueueManagerName());		
		Thread.sleep(2000);

		assert (conn != null) : "MQ connection object has not been created";

		//MQQueueManager qm = conn.getMQQueueManager();
		//log.info("Return code: " + conn.getReasonCode());

		assert (conn.QueueManagerEventsObject() != null) : "Queue Manager Event object not created successfully";
		assert (conn.ChannelEventsObject() != null) : "Channel Event object not created successfully";
		assert (conn.CPUEventsObject() != null) : "CPU Event object not created successfully";
		assert (conn.ConfigEventsObject() != null) : "Config Event object not created successfully";
				
	}
	
	//@Test
	//@Order(2)
	//public void testConnectionToTheQueueManager() {

		/*
		log.info("Queue manager connection");
		String mess = "";
		
		try {
			
			mess = "Queue manager";
			MQQueueManager qm = getQueMan().createQueueManager();
			assert (qm) != null;
			
		} catch (Exception e) {
			log.info("Error: " + mess);
			e.printStackTrace();
			
		}
		*/
	//}

	
	//@Test
	//@Order(3)
	//public void createPCFMessageAgent() {

	//	log.info("Queue manager connection");
	//	String mess = "";
		/*
		try {
			
			mess = "Queue manager";
			MQQueueManager qm = getQueMan().createQueueManager();
			assert (qm) != null;

			mess = "PCF Agent";
			PCFMessageAgent ag = getQueMan().createMessageAgent(qm);
			assert (ag) != null;
			
			int qmgrAcct = getQueMan().getAccounting();
			
		} catch (Exception e) {
			log.info("Error: " + mess);
			e.printStackTrace();
			
		}
		*/
	//}
}
