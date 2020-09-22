package maersk.com.mq.channelevents;

/*
 * Copyright 2020
 * Maersk
 *
 * Get MQ Channel events
 * 
 * 01/07/2020 - Get MQ Channel events
 * 
 */

import java.io.IOException;
import java.net.MalformedURLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.pcf.PCFException;
import com.ibm.mq.headers.pcf.PCFMessage;
import com.ibm.mq.headers.pcf.PCFMessageAgent;
import com.ibm.mq.headers.pcf.PCFParameter;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import maersk.com.mq.events.IEvents;
import maersk.com.mq.events.MQMetricsQueueManager;
import maersk.com.mq.events.IMQPCFConstants;

@Component
public class ChannelEvents implements Callable<Integer>, IEvents, IMQPCFConstants {

	private final static Logger log = LoggerFactory.getLogger(ChannelEvents.class);

	@Autowired
	public MeterRegistry meterRegistry;
	
    private MQQueueManager queManager;
    public void QueueManager(MQQueueManager qm) {
    	this.queManager = qm;
    }
    public MQQueueManager QueueManager() {
    	return this.queManager;
    }
	    
	private String queuemanagername;
	public void QueueManagerName(String v) {
		this.queuemanagername = v;
	}
	public String QueueManagerName() {
		return this.queuemanagername;
	}
	
    @Value("${ibm.mq.event.delayInMilliSeconds}")
	private int resetIterations;

    private PCFMessageAgent messageAgent = null;
    private PCFMessageAgent getMessageAgent() {
    	return this.messageAgent;
    }

    @Autowired
    private MQMetricsQueueManager mqMetricsQueueManager;
    public MQMetricsQueueManager MetricsQueueManager() {
    	return this.mqMetricsQueueManager;
    }

    @Value("${ibm.mq.event.channel.queue:SYSTEM.ADMIN.CHANNEL.EVENT}")
    private String queueName;
    public String QueueName() {
    	return this.queueName;	
    }
    
    private MQQueue queue;
    public MQQueue Queue() {
    	return this.queue;
    }
    public void Queue(MQQueue v) {
    	this.queue = v;
    }

    private MQGetMessageOptions gmo;
    public MQGetMessageOptions GetMessageOptions() {
    	return this.gmo;
    }
    public void GetMessageOptions(MQGetMessageOptions v) {
    	this.gmo = v;
    }

    @Value("#{new Boolean('${ibm.mq.broswe:true}')}")
    private Boolean browse;
    public Boolean Browse() {
    	return this.browse;
    }

    @Value("#{new Integer('${ibm.mq.event.waitInterval:5000}')}")    
    private Integer waitinterval;
    public Integer WaitInterval() {
    	return this.waitinterval;
    }
    
    private Map<String,AtomicLong>ChannelEventsMap = new HashMap<String,AtomicLong>();
    protected static final String lookupChannelStart = "mq:channelStart";
    	
	private int taskstatus;
	private void TaskStatus(int v) {
		this.taskstatus = v;
	}
	public int TaskStatus() {
		return this.taskstatus;
	}

	private int retcode;
	public void RetCode(int v) {
		this.retcode = v;
	}
	public int RetCode() {
		return this.retcode;
	}
	
	private GregorianCalendar cal;
	private int hour;
	private void Hour(int v) {
		this.hour = v;
	}
	private int Hour() {
		return this.hour;
	}
	private int day;
	private void Day(int v) {
		this.day = v;
	}
	private int Day() {
		return this.day;
	}
	private int month;
	private void Month(int v) {
		this.month = v;
	}
	private int Month() {
		return this.month;
	}
	private int year;
	private void Year(int v) {
		this.year = v;
	}
	private int Year() {
		return this.year;
	}
	
	// Constructor
    public ChannelEvents() {
    }

    @PostConstruct
    public void Init()  {
    	log.info("channelevents: Init");    	
    	TaskStatus(IMQPCFConstants.TASK_STOPPED);
    }
    
    /**
     * Override the initial call method and run the task
     * Return the status of the task
     */
    @Override
	public Integer call() throws MQException, MQDataException, IOException, InterruptedException {

    	QueueManager(MetricsQueueManager().CreateQueueManager());
		QueueManagerName(QueueManager().getName().trim());
		RetCode(IMQPCFConstants.OKAY);
		
    	RunTask();
    	return RetCode();
	}

    /**
     * Run the task
     */
    @Override
	public void RunTask() throws MQException, MQDataException, IOException {
    	log.debug("channelevents: RunTask");

    	/*
    	 * Open the queue
    	 */
		try {
			Queue(null);
			OpenQueueForReading();
			
		} catch (MQException e) {
			log.error("Unable to open queue {}, reason: {}", QueueName(), e.getReason());
			RetCode(e.getReason());
			throw new MQException(e.getCompCode(), e.getReason(), e);
		}
		
		/*
		 * Process messages
		 */
		try {
			ReadEventsFromQueue();
			
		} catch (MQException e) {
			log.debug("Unable to read messages from " + QueueName());
			RetCode(e.getReason());
			
		} catch (MQDataException e) {
			log.debug("Unable to read messages from " + QueueName());
			RetCode(e.getReason());
			
		} catch (IOException e) {
			log.debug("Unable to read messages from " + QueueName());
			RetCode(IMQPCFConstants.RET_WITH_ERROR);
		}
	}
    
	/**
	 * Open the queue for reading ...
	 */
    @Override
	public void OpenQueueForReading() throws MQException {

		log.debug("opening queue for reading");
		int openOptions = MQConstants.MQOO_INPUT_AS_Q_DEF |
				MQConstants.MQOO_BROWSE |
				MQConstants.MQOO_FAIL_IF_QUIESCING;			

		GetMessageOptions(new MQGetMessageOptions());		
		Queue(QueueManager().accessQueue(QueueName(), openOptions));
		SetGMOOptions(MQConstants.MQGMO_BROWSE_FIRST);
		
	}
    
    /**
     * Read messages and process
     * 
     * While the task is starting or running
     * ... get messages from the queue
     * ........ build the PCF message and extract the values
     */
    @Override
	public void ReadEventsFromQueue() throws MQException, MQDataException, IOException {

		TaskStatus(IMQPCFConstants.TASK_STARTING);
		String channelName = "";
		
		while ((TaskStatus() == IMQPCFConstants.TASK_STARTING)
				|| (TaskStatus() == IMQPCFConstants.TASK_RUNNING)) {
			
			if (TaskStatus() == IMQPCFConstants.TASK_STARTING) {
				TaskStatus(IMQPCFConstants.TASK_RUNNING);
			}
			
			if (Thread.interrupted()) {
				TaskStatus(IMQPCFConstants.TASK_STOPPING);
			}
						
			try {		
				MQMessage message = new MQMessage ();
				Queue().get (message, GetMessageOptions());
	
				if (message.format.equals(MQConstants.MQFMT_EVENT)) {
					calcDayBucket(message.putDateTime);
					
					PCFMessage pcf = new PCFMessage (message);
					int event = pcf.getReason();
					String eventName = MQConstants.lookup(event, "MQRC_.*").trim();
					
					int reason = 0;
					String status = "";
					try {
						reason = pcf.getIntParameterValue(MQConstants.MQIACF_REASON_QUALIFIER);
						status = MQConstants.lookup(reason, "MQRQ_.*").trim();
					
					} catch (Exception e) {
					}
					
					try {
						channelName = pcf.getStringParameterValue(MQConstants.MQCACH_CHANNEL_NAME);
					
					} catch (Exception e) {
						channelName = "UNKNOWN";
					}

					StringBuilder sb = new StringBuilder();
					sb.append(lookupChannelStart + "_");
					sb.append(Hour() + "_");
					sb.append(Day() + "_");
					sb.append(Month() + "_");
					sb.append(Year() + "_");
					sb.append(eventName + "_");
					sb.append(status + "_");
					sb.append(channelName + "_");
					sb.append(QueueName());
					
					String label = sb.toString();

					AtomicLong v = this.ChannelEventsMap.get(label);
					if (v == null) {
						ChannelEventsMap.put(label, 
							meterRegistry.gauge(lookupChannelStart, 
							Tags.of("queueManagerName", QueueManagerName(),
									"eventName",eventName,
									"status", status,
									"channelName",channelName,
									"hour", Integer.toString(Hour()),
									"day",Integer.toString(Day()),
									"month",Integer.toString(Month()),
									"year",Integer.toString(Year())
							),
								new AtomicLong(1))
							);
						log.debug("Channel event created ...");
					} else {
						v.incrementAndGet();
					}									
					
				}
				DeleteMessagesUnderCursor();
				SetGMOOptions(MQConstants.MQGMO_BROWSE_NEXT);
								
			} catch (MQException e) {
				if (e.getReason() == MQConstants.MQRC_NO_MSG_AVAILABLE) {
					log.debug("No more channel event messages");
					RetCode(e.getReason());
					
				} else {
					if (Thread.interrupted()) {
						log.error("Thread interupted");						
					}
					if (e.getReason() != MQConstants.MQRC_UNEXPECTED_ERROR) {
						RetCode(e.getReason());
						log.error("MQ error : {} reasonCode {}", e.getMessage(), e.reasonCode);
						
					} else {
						RetCode(IMQPCFConstants.OKAY);						
					}
					TaskStatus(IMQPCFConstants.TASK_STOPPING);
					
				}
				
			} catch (MQDataException e) {
					log.warn("Unknown property in channel event messages");
					RetCode(e.getReason());
			}
    		
			
		} // end of while 
		
		TaskStatus(IMQPCFConstants.TASK_STOPPED);
		RetCode(IMQPCFConstants.OKAY);
		log.debug("Channel task stopped");

    }

    /*
     * Set the GetMessageOptions
     */
    private void SetGMOOptions(int firstNext) {
    	
		int options = MQConstants.MQGMO_WAIT
				| MQConstants.MQGMO_FAIL_IF_QUIESCING
				| MQConstants.MQGMO_CONVERT;
		GetMessageOptions().waitInterval = WaitInterval();
		
		if (Browse()) {
			if (firstNext == MQConstants.MQGMO_BROWSE_FIRST) {
				options += MQConstants.MQGMO_BROWSE_FIRST;
				
			} else {
				if (firstNext == MQConstants.MQGMO_BROWSE_NEXT) {
					options += MQConstants.MQGMO_BROWSE_NEXT;					
				}
			}

		} else {
			options = MQConstants.MQGMO_WAIT
					| MQConstants.MQGMO_FAIL_IF_QUIESCING
					| MQConstants.MQGMO_CONVERT
					| MQConstants.MQGMO_SYNCPOINT;
		}
		GetMessageOptions().options = options;
		
	}

    /*
     * Calculate the dates
     */
	private void calcDayBucket(GregorianCalendar putDateTime) {
		
		ZonedDateTime zdt;
		Instant instant;

		this.cal = putDateTime;
		zdt = this.cal.toZonedDateTime();
		instant = zdt.toInstant();
		ZonedDateTime z = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);

		Hour(this.cal.get(Calendar.HOUR_OF_DAY)); 
		Day(this.cal.get(Calendar.DAY_OF_MONTH));
		Month((this.cal.get(Calendar.MONTH) + 1)); // Month is indexed from 0 !!, so, JAN = 0, FEB = 1 etc 
		Year(this.cal.get(Calendar.YEAR));
		
		
	}
	
	/*
	 * Delete the message if needed
	 */
	@Override
	public void DeleteMessagesUnderCursor() throws MQException {

		/*
		 * For the queue we are looking for ...
		 *    if we want to, remove the message from the accounting queue
		 */
		if (!Browse()) {
			MQMessage message = new MQMessage ();		
			GetMessageOptions().options = MQConstants.MQGMO_MSG_UNDER_CURSOR 
					| MQConstants.MQGMO_WAIT
					| MQConstants.MQGMO_FAIL_IF_QUIESCING
					| MQConstants.MQGMO_CONVERT;
			try {
				Queue().get (message, GetMessageOptions());
				log.debug("Deleting message ...." );

			} catch (Exception e) {
				/*
				 * If we fail, then someone else might has removed
				 * the message, so continue
				 */
			}
		} else {
			QueueManager().commit();

		}
		
	}
		
	/*
	 * Destroy the object
	 */
	@Override
	@PreDestroy	
    public void Destroy() {
    	log.info("Destroy Channel Events");
    	int maxCount = 0;
    	
    	do {
    		TaskStatus(IMQPCFConstants.TASK_STOPPING);
	    	maxCount++;
	    	
    	} while (TaskStatus() == IMQPCFConstants.TASK_RUNNING || maxCount > 10);
    	
    	try {
	    	if (Queue() != null) {
	    		MetricsQueueManager().CloseQueue(Queue());
	    	}
	    	if (QueueManager() != null) {
	    		MetricsQueueManager().CloseConnection(QueueManager());
	    	}
	    	
    	} catch (Exception e) {
    		// continue if we can an error
    	}
    }

	
}
