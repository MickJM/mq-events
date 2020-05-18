package maersk.com.mq.events;

import java.io.IOException;
import java.net.MalformedURLException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.ibm.mq.MQException;
import com.ibm.mq.headers.MQDataException;

public interface IQueueManagerEvents {

	@PostConstruct
	public void init() throws MQException, MalformedURLException, MQDataException;

	public void runTask() throws MQException, MQDataException, IOException, InterruptedException;
	
	public void readEventsFromQueue() throws MQException, MQDataException, IOException, InterruptedException;

	public void openQueueForReading() throws MQException;
	
	public void deleteMessagesUnderCursor() throws MQException;
	
	@PreDestroy
	public void destroy() throws MQException, InterruptedException;
	
}
