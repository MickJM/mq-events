package maersk.com.mq.events;

import java.io.IOException;
import java.net.MalformedURLException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.ibm.mq.MQException;
import com.ibm.mq.headers.MQDataException;

public interface IEvents {

	@PostConstruct
	public void Init() throws MQException, MalformedURLException, MQDataException;

	public void RunTask() throws MQException, MQDataException, IOException, InterruptedException;
	
	public void ReadEventsFromQueue() throws MQException, MQDataException, IOException, InterruptedException;

	public void OpenQueueForReading() throws MQException;
	
	public void DeleteMessagesUnderCursor() throws MQException;
	
	@PreDestroy
	public void Destroy() throws MQException, InterruptedException;
	
}
