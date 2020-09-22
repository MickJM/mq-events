package maersk.com.mq.queuemanagerevents;

/*
 * Copyright 2020
 * Maersk
 *
 * https://community.ibm.com/community/user/imwuc/viewdocument/a-first-look-at-mq-resource-usage-s?CommunityKey=183ec850-4947-49c8-9a2e-8e7c7fc46c64&tab=librarydocuments
 *
 * Get MQ Queue manager events
 * 
 * 01/07/2020 - Get MQ queue manager events
 *            - from queue SYSTEM.ADMIN.QMGR.EVENT
 * 
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
import org.springframework.beans.factory.annotation.Value;
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
import maersk.com.mq.events.IEvents;
import maersk.com.mq.events.MQMetricsQueueManager;
import maersk.com.mq.events.IMQPCFConstants;

@Component
public class QueueMangerEvents implements Callable<Integer>, IEvents {

    private final static Logger log = LoggerFactory.getLogger(QueueMangerEvents.class);

	@Autowired
	public MeterRegistry meterRegistry;

    @Autowired
    private MQMetricsQueueManager metricsqueuemanager;
    private MQMetricsQueueManager MetricsQueueManager() {
    	return this.metricsqueuemanager;
    }
	
    //@Autowired
    private MQQueueManager queManager;
    public void QueueManager(MQQueueManager qm) {
    	this.queManager = qm;
    }
    public MQQueueManager QueueManager() {
    	return this.queManager;
    }

    @Value("${ibm.mq.event.queuemanager.queue:SYSTEM.ADMIN.QMGR.EVENT}")
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

    private String queueManagerName;
	private void QueueManagerName(String v) {
		this.queueManagerName = v;
	}
    private String QueueManagerName() {
    	return this.queueManagerName;
    }
    
    private Map<String,AtomicLong>queueManagerEventsMap = new HashMap<String,AtomicLong>();
	protected static final String lookupQueueManagerEvents = "mq:queuemanagerevents";
    
	private int status;
	private void TaskStatus(int v) {
		this.status = v;
	}
	public int TaskStatus() {
		return this.status;
	}
    
	private int retCode;
	public void RetCode(int v) {
		this.retCode = v;
	}
	public int RetCode() {
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
    public void Init() throws MQException, MalformedURLException, MQDataException {
    	log.info("stopStartEvents: Init");
    	TaskStatus(IMQPCFConstants.TASK_STOPPED);

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
		
    	QueueManager(MetricsQueueManager().CreateQueueManager());
		QueueManagerName(QueueManager().getName().trim());
		RetCode(IMQPCFConstants.OKAY);
		
		RunTask();
		return RetCode();
		
	}

	@Override
	public void RunTask() throws MQException, MQDataException, IOException, InterruptedException {
		log.debug("stopStartEvents: RunTask");

		/*
		 * Open the queue
		 */
		try {
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
			log.info("Unable to read messages from {}", QueueName());
			RetCode(e.getReason());
			throw new MQException(e.getCompCode(), e.getReason(), e);
			
		} catch (MQDataException e) {
			log.info("Unable to read messages from {}", QueueName());
			RetCode(e.getReason());
			throw new MQDataException(e.getCompCode(), e.getReason(), e);

		} catch (IOException e) {
			log.info("Unable to read messages from {}", QueueName());
			RetCode(IMQPCFConstants.RET_WITH_ERROR);
			throw new IOException(e);
			
		}
		
	}    
	
	/**
	 * Open the queue for reading in browse mode ...
	 * Messages will be deleted if needed using UNDER_CURSOR
	 * 
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
	 * Read messages from the queue
	 * 
	 * While the task is starting or running 
	 * ... read messages from the queue
	 * ....... if a PCFEvent message,
	 * .............. create the metric 
	 */
	@Override
	public void ReadEventsFromQueue() throws MQException, MQDataException, IOException, InterruptedException {
		
		log.debug("Start reading events ...");
		TaskStatus(IMQPCFConstants.TASK_STARTING);
		
		MQMessage message = new MQMessage ();
		
		while ((TaskStatus() == IMQPCFConstants.TASK_STARTING)
				|| (TaskStatus() == IMQPCFConstants.TASK_RUNNING)) {
			
			if (TaskStatus() == IMQPCFConstants.TASK_STARTING) {
				TaskStatus(IMQPCFConstants.TASK_RUNNING);
			}

			if (Thread.interrupted()) {
				TaskStatus(IMQPCFConstants.TASK_STOPPING);
				
			}
						
			try {
				message.messageId = MQConstants.MQMI_NONE;
				message.correlationId = MQConstants.MQMI_NONE;
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
					sb.append(QueueManagerName());

					//final String label = lookupStartAndStopEvents + "_" + eventName + "_" + getQueueManagerName();					
					String label = sb.toString();
					
					AtomicLong put = queueManagerEventsMap.get(label);
					if (put == null) {
						queueManagerEventsMap.put(label, meterRegistry.gauge(lookupQueueManagerEvents, 
								Tags.of("queueManagerName", QueueManagerName(),
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
						log.debug("QueueManager event created ...");
						
					} else {
						put.incrementAndGet();
					}
					
					// 31 - failover not permitted					
				}
				
				DeleteMessagesUnderCursor();
				SetGMOOptions(MQConstants.MQGMO_BROWSE_NEXT);
				
			} catch (MQException e) {
				if (e.getReason() == MQConstants.MQRC_NO_MSG_AVAILABLE) {
					log.debug("No more queue manager start/stop messages");
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
				log.info("Unknown property in start/stop event messages");
				RetCode(e.getReason());

			}

		}
		
		TaskStatus(IMQPCFConstants.TASK_STOPPED);
		RetCode(IMQPCFConstants.OKAY);
		log.info("StartStop task stopped");

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
     * Calculate the date
     */
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
		setMonth((this.cal.get(Calendar.MONTH) + 1)); // Month is indexed from 0 !!, so, JAN = 0, FEB = 1 etc 
		setYear(this.cal.get(Calendar.YEAR));
	}
	
	/*
	 * Delete the message if needed
	 */
	@Override
	public void DeleteMessagesUnderCursor() {

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
		}
		
	}
	
	/*
	 * Destroy the object
	 */
	@Override
	@PreDestroy
    public void Destroy()  {
    	log.info("Destroy QueueManager Events");
    	int maxCount = 0;
    	
    	do {
    		TaskStatus(IMQPCFConstants.TASK_STOPPING);
	    	maxCount++;
	    
    	} while (TaskStatus() == IMQPCFConstants.TASK_STOPPED || maxCount > 10);
    	
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
