package maersk.com.mq.queuemanagerevents;

/*
 * Process ADMIN queue manager events
 */

import java.io.IOException;
import java.net.MalformedURLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/*
 * Copyright 2019
 * Get channel details
 * 
 */

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.pcf.PCFMessage;
import com.ibm.mq.headers.pcf.PCFParameter;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import maersk.com.mq.events.IQueueManagerEvents;
import maersk.com.mq.events.MQMetricsQueueManager;
import maersk.com.mq.events.MQPCFConstants;

@Component
public class QueueMangerEvents implements Callable<Integer>, IQueueManagerEvents {

    private final static Logger log = LoggerFactory.getLogger(QueueMangerEvents.class);

	@Autowired
	public MeterRegistry meterRegistry;

    @Autowired
    private MQMetricsQueueManager mqMetricsQueueManager;
    private MQMetricsQueueManager getMetricQueueManager() {
    	return this.mqMetricsQueueManager;
    }
	
    //@Autowired
    private MQQueueManager queManager;
    public void setQueueManager(MQQueueManager qm) {
    	this.queManager = qm;
    }
    public MQQueueManager getQueueManager() {
    	return this.queManager;
    }
        
    private final String queueName = "SYSTEM.ADMIN.QMGR.EVENT";
    private MQQueue queue;
    public MQQueue getQueue() {
    	return this.queue;
    }
    public void setQueue(MQQueue v) {
    	this.queue = v;
    }
    
    private MQGetMessageOptions gmo;
    public MQGetMessageOptions getGMO() {
    	return this.gmo;
    }
    public void setGMO(MQGetMessageOptions v) {
    	this.gmo = v;
    }
    
    private boolean browse = true;
    public boolean getBrowse() {
    	return this.browse;
    }

    private String queueManagerName;
	private void setQueueManagerName(String v) {
		this.queueManagerName = v;
	}
    private String getQueueManagerName() {
    	return this.queueManagerName;
    }
    
    private Map<String,AtomicLong>queueManagerEventsMap = new HashMap<String,AtomicLong>();
	protected static final String lookupQueueManagerEvents = "mq:queuemanagerevents";
    
	private int status;
	private void taskStatus(int v) {
		this.status = v;
	}
	public int taskStatus() {
		return this.status;
	}
    
	private int retCode;
	public void setRetCode(int v) {
		this.retCode = v;
	}
	public int getRetCode() {
		return this.retCode;
	}
	
	private GregorianCalendar cal;
	private int hour;
	private void setHour(int v) {
		this.hour = v;
	}
	private int getHour() {
		return this.hour;
	}
	private int day;
	private void setDay(int v) {
		this.day = v;
	}
	private int getDay() {
		return this.day;
	}
	private int month;
	private void setMonth(int v) {
		this.month = v;
	}
	private int getMonth() {
		return this.month;
	}
	private int year;
	private void setYear(int v) {
		this.year = v;
	}
	private int getYear() {
		return this.year;
	}
	
    /*
     * Constructor ...
     */
    public QueueMangerEvents() {		
    }

    /**
     * When the class is fully created ...
     * @throws MQDataException 
     * @throws MalformedURLException 
     * 
     */
    @PostConstruct
    @Override
    public void init() {

    	log.info("stopStartEvents: init");
    	taskStatus(MQPCFConstants.TASK_STOPPED);

    }

    /**
     * Start the task and return the apprioriate response value
     * @throws MQDataException 
     * @throws MQException 
     * @throws InterruptedException 
     * @throws IOException 
     * 
     */
	@Override
	public Integer call() throws MQException, MQDataException, IOException, InterruptedException {

    	setQueueManager(getMetricQueueManager().createQueueManager());

		setQueueManagerName(getQueueManager().getName().trim());
		setRetCode(0);
		
		runTask();
		return getRetCode();
		
	}

	@Override
	public void runTask() throws MQException, MQDataException, IOException, InterruptedException {
		log.info("stopStartEvents: run");

		/*
		 * Open the queue
		 */
		try {
			openQueueForReading();
			
		} catch (MQException e) {
			log.info("Unable to open queue " + this.queueName);
			setRetCode(e.getReason());
			throw new MQException(e.getCompCode(), e.getReason(), e);
		}
	
		/*
		 * Process messages
		 */
		try {
			readEventsFromQueue();
			
		} catch (MQException e) {
			log.info("Unable to read messages from " + this.queueName);
			setRetCode(e.getReason());
			throw new MQException(e.getCompCode(), e.getReason(), e);
			
		} catch (MQDataException e) {
			log.info("Unable to read messages from " + this.queueName);
			setRetCode(e.getReason());
			throw new MQDataException(e.getCompCode(), e.getReason(), e);

		} catch (IOException e) {
			log.info("Unable to read messages from " + this.queueName);
			setRetCode(20);
			throw new IOException(e);
			
		}
		
	}    
	
	/**
	 * Open the queue for reading in browse mode ...
	 * Messages will be deleted if needed using UNDER_CURSOR
	 * 
	 */
	@Override
	public void openQueueForReading() throws MQException {

		log.info("opening queue for reading");	
		setQueue(null);
		if (getQueue() == null) {
			int openOptions = MQConstants.MQOO_INPUT_AS_Q_DEF |
					MQConstants.MQOO_BROWSE |
					MQConstants.MQOO_FAIL_IF_QUIESCING;			

			setGMO(new MQGetMessageOptions());
			setQueue(getQueueManager().accessQueue(queueName, openOptions));

		}
		
		int gmoptions = MQConstants.MQGMO_NO_WAIT 
				| MQConstants.MQGMO_FAIL_IF_QUIESCING
				| MQConstants.MQGMO_CONVERT;
		
		if (getBrowse()) {
			gmoptions = gmoptions | MQConstants.MQGMO_BROWSE_FIRST;

		} 
		getGMO().options = gmoptions;
		getGMO().waitInterval = 5000;
	//	getGMO().matchOptions = MQConstants.MQMO_MATCH_MSG_ID  | MQConstants.MQMO_MATCH_CORREL_ID;
		
	}

	/**
	 * Read messages from the queue
	 * 
	 * While the task is starting or running 
	 * ... read messages from the queue
	 * ....... if a PCFEvent message,
	 * .............. create the metric 
	 */
	@Override
	public void readEventsFromQueue() throws MQException, MQDataException, IOException, InterruptedException {
		
		log.info("Start reading events ...");
		taskStatus(MQPCFConstants.TASK_STARTING);
		
		MQMessage message = new MQMessage ();
		
		while ((taskStatus() == MQPCFConstants.TASK_STARTING)
				|| (taskStatus() == MQPCFConstants.TASK_RUNNING)) {
			
			if (taskStatus() == MQPCFConstants.TASK_STARTING) {
				taskStatus(MQPCFConstants.TASK_RUNNING);
			}

			if (Thread.interrupted()) {
				taskStatus(MQPCFConstants.TASK_STOPPING);
				
			}
						
			try {
				message.messageId = MQConstants.MQMI_NONE;
				message.correlationId = MQConstants.MQMI_NONE;
				getQueue().get (message, getGMO());
	
				if (message.format.equals(MQConstants.MQFMT_EVENT)) {
					calcDayBucket(message.putDateTime);

					PCFMessage pcf = new PCFMessage (message);
	
					//Enumeration<PCFParameter> parms = pcf.getParameters();
					
					//int reason = pcf.getIntParameterValue(MQConstants.MQIACF_REASON_QUALIFIER);
					//String status = MQConstants.lookup(reason, "MQRQ_.*");

					int event = pcf.getReason();
					String eventName = MQConstants.lookup(event, "MQRC_.*").trim();
					
					int reason = 0;
					String status = "";
					
					try {
						reason = pcf.getIntParameterValue(MQConstants.MQIACF_REASON_QUALIFIER);
						status = MQConstants.lookup(reason, "MQRQ_.*").trim();
					
					} catch (Exception e) {
					}					

					String user = "";
					try {
						user = pcf.getStringParameterValue(MQConstants.MQCACF_CSP_USER_IDENTIFIER).trim();
					
					} catch (Exception e) {
					}					

					String app = "";
					try {
						app = pcf.getStringParameterValue(MQConstants.MQCACF_APPL_NAME).trim();
					
					} catch (Exception e) {
					}					
					
					StringBuilder sb = new StringBuilder();
					sb.append(lookupQueueManagerEvents + "_");
					sb.append(getHour() + "_");
					sb.append(getDay() + "_");
					sb.append(getMonth() + "_");
					sb.append(getYear() + "_");
					sb.append(eventName + "_");
					sb.append(status + "_");
					sb.append(getQueueManagerName());

					//final String label = lookupStartAndStopEvents + "_" + eventName + "_" + getQueueManagerName();					
					String label = sb.toString();
					
					AtomicLong put = queueManagerEventsMap.get(label);
					if (put == null) {
						queueManagerEventsMap.put(label, meterRegistry.gauge(lookupQueueManagerEvents, 
								Tags.of("queueManagerName", getQueueManagerName(),
										"eventName",eventName,
										"status", status,
										"user",user,
										"application",app,
										"hour", Integer.toString(getHour()),
										"day",Integer.toString(getDay()),
										"month",Integer.toString(getMonth()),
										"year",Integer.toString(getYear())
										
										),
								new AtomicLong(1))
								);
						log.info("Event created ...");
						
					} else {
						put.incrementAndGet();
				//		put.set(reason);		
					}
					
					// 31 - failover not permitted					
				}
				
				deleteMessagesUnderCursor();

				getGMO().options = MQConstants.MQGMO_NO_WAIT
						| MQConstants.MQGMO_FAIL_IF_QUIESCING
						| MQConstants.MQGMO_CONVERT;
				
				if (getBrowse()) {
					getGMO().options = getGMO().options | MQConstants.MQGMO_BROWSE_NEXT;
				}
					
				
			} catch (MQException e) {
				if (e.getReason() == MQConstants.MQRC_NO_MSG_AVAILABLE) {
				//	log.info("No more start/stop messages");
					setRetCode(e.getReason());

				} else {
					if (Thread.interrupted()) {
						log.error("Thread interupted");
						log.error("MQ error : {} reasonCode {}", e.getMessage(), e.reasonCode);
						setRetCode(e.getReason());

					} else {
						if (e.getReason() != MQConstants.MQRC_UNEXPECTED_ERROR) {
							log.error("MQ error : {} reasonCode {}", e.getMessage(), e.reasonCode);
						}
						setRetCode(e.getReason());
						
					}
					taskStatus(MQPCFConstants.TASK_STOPPING);
				}
				
			} catch (MQDataException e) {
				log.info("Unknown property in start/stop event messages");
				setRetCode(e.getReason());

			}

			
		}
		taskStatus(MQPCFConstants.TASK_STOPPED);
		setRetCode(0);
		log.info("StartStop task stopped");

	}
	
	private void calcDayBucket(GregorianCalendar putDateTime) {
		ZonedDateTime zdt;
		Instant instant;

		this.cal = putDateTime;
		zdt = this.cal.toZonedDateTime();
		instant = zdt.toInstant();
		ZonedDateTime z = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
		long timeInMilli = z.toInstant().toEpochMilli();

		setHour(this.cal.get(Calendar.HOUR_OF_DAY)); 
		setDay(this.cal.get(Calendar.DAY_OF_MONTH));
		//int weekOfYear = this.cal.get(Calendar.WEEK_OF_YEAR);
		setMonth((this.cal.get(Calendar.MONTH) + 1)); // Month is indexed from 0 !!, so, JAN = 0, FEB = 1 etc 
		setYear(this.cal.get(Calendar.YEAR));
	}
	
	/**
	 * Delete message
	 * 
	 */
	@Override
	public void deleteMessagesUnderCursor() {

		/*
		 * For the queue we are looking for ...
		 *    if we want to, remove the message from the accounting queue
		 */
		if (!getBrowse()) {
			MQMessage message = new MQMessage ();		
			getGMO().options = MQConstants.MQGMO_MSG_UNDER_CURSOR 
					| MQConstants.MQGMO_WAIT
					| MQConstants.MQGMO_FAIL_IF_QUIESCING
					| MQConstants.MQGMO_CONVERT;
			try {
				getQueue().get (message, getGMO());
				log.debug("Deleting message ...." );

			} catch (Exception e) {
				/*
				 * If we fail, then someone else might has removed
				 * the message, so continue
				 */
			}
		}
		
	}
	
	/**
	 * Triggered from the main thred - set taskStatus to STOPPING
	 */
    @PreDestroy
    public void destroy()  {
    	log.info("queueManagerEvents: destroy");
    	int maxCount = 0;
    	
    	do {
    		taskStatus(MQPCFConstants.TASK_STOPPING);
	    	maxCount++;
	    
    	} while (taskStatus() == MQPCFConstants.TASK_STOPPED || maxCount > 10);
    	
    	try {
	    	if (getQueueManager() != null) {
	    		getQueueManager().disconnect();
	    	}
	    	
	    	if (getQueue() != null) {
	    		getQueue().close();
	    	}
    	} catch (Exception e) {
    		// continue if we can an error
    	}
    
    }
    
}
