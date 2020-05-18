package maersk.com.mq.channelevents;

/*
 * Copyright 2019
 * Mick Moriarty - Maersk
 *
 * Get queue manager details
 * 
 * 22/10/2019 - When the queue manager is not running, check if the status is multi-instance
 *              and set the status accordingly
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
import maersk.com.mq.events.IQueueManagerEvents;
import maersk.com.mq.events.MQMetricsQueueManager;
import maersk.com.mq.events.MQMonitorBase;
import maersk.com.mq.events.MQPCFConstants;

@Component
public class ChannelEvents implements Callable<Integer>, IQueueManagerEvents {

	private final static Logger log = LoggerFactory.getLogger(ChannelEvents.class);

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
	    
	private String queueManager;
	public void setQueueManagerName(String v) {
		this.queueManager = v;
	}
	public String getQueueManagerName() {
		return this.queueManager;
	}
	
    @Value("${ibm.mq.event.delayInMilliSeconds}")
	private int resetIterations;

    private PCFMessageAgent messageAgent = null;
    private PCFMessageAgent getMessageAgent() {
    	return this.messageAgent;
    }

    @Autowired
    private MQMonitorBase base;
    
    @Autowired
    private MQMetricsQueueManager metqm;
    
    private final String queueName = "SYSTEM.ADMIN.CHANNEL.EVENT";
    public String getQueueName() {
    	return this.queueName;	
    }
    
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
    
    private Map<String,AtomicLong>ChannelEventsMap = new HashMap<String,AtomicLong>();

    protected static final String lookupChannelStart = "mq:channelStart";
	protected static final String lookupChannelStartByQueue = "mq:channelStartByQueue";

	protected static final String lookupChannelStop = "mq:channelStop";
    	
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
	
	// Constructor
    public ChannelEvents() {
    }

    @PostConstruct
    public void init()  {
    	log.info("channelevents: init");    	
    	taskStatus(MQPCFConstants.TASK_STOPPED);
    	
    }
    
    /**
     * Override the initial call method and run the task
     * Return the status of the task
     */
    @Override
	public Integer call() throws MQException, MQDataException, IOException, InterruptedException {

    	setQueueManager(getMetricQueueManager().createQueueManager());
    	
		setQueueManagerName(getQueueManager().getName().trim());
		setRetCode(0);
		
    	runTask();
    	return getRetCode();
	}

    /**
     * Run the task
     * 
     */
    @Override
	public void runTask() throws MQException, MQDataException, IOException {

    	log.info("channelevents: run");

    	/*
    	 * Open the queue
    	 */
		try {
			openQueueForReading();
			
		} catch (MQException e) {
			log.info("Unable to open queue " + getQueueName());
		}
		
		/*
		 * Process messages
		 */
		try {
			readEventsFromQueue();
			
		} catch (MQException e) {
			log.info("Unable to read messages from " + getQueueName());
			setRetCode(e.getReason());
			
		} catch (MQDataException e) {
			log.info("Unable to read messages from " + getQueueName());
			setRetCode(e.getReason());
			
		} catch (IOException e) {
			log.info("Unable to read messages from " + this.queueName);
			setRetCode(20);
		}
		
    
		
	}
    
	/**
	 * Open the queue for reading ...
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

		if (getBrowse()) {
			getGMO().options = MQConstants.MQGMO_BROWSE_FIRST 
					| MQConstants.MQGMO_NO_WAIT
					| MQConstants.MQGMO_FAIL_IF_QUIESCING
					| MQConstants.MQGMO_CONVERT;
			
		} else {
			getGMO().options = MQConstants.MQGMO_NO_WAIT
					| MQConstants.MQGMO_FAIL_IF_QUIESCING
					| MQConstants.MQGMO_CONVERT
					| MQConstants.MQGMO_SYNCPOINT;
		}
		
		//int gmoptions = MQConstants.MQGMO_WAIT 
		//		| MQConstants.MQGMO_FAIL_IF_QUIESCING
		//		| MQConstants.MQGMO_CONVERT;
		
		//if (getBrowse()) {
		//	gmoptions = gmoptions | MQConstants.MQGMO_BROWSE_FIRST;

		//} 
		//getGMO().options = gmoptions;
	//	getGMO().waitInterval = 5000;
	//	getGMO().matchOptions = MQConstants.MQMO_MATCH_MSG_ID  | MQConstants.MQMO_MATCH_CORREL_ID;
		
	}
    
    /**
     * Read messages and process
     * 
     * While the task is starting or running
     * ... get messages from the queue
     * ........ build the PCF message and extract the values
     */
    @Override
	public void readEventsFromQueue() throws MQException, MQDataException, IOException {

		taskStatus(MQPCFConstants.TASK_STARTING);
		
		String channelName;
		String xmitQ;
		
		ZonedDateTime zdt;
		Instant instant;
		
		final Map<String,Integer>eventMap = new HashMap<String, Integer>();
		
		while ((taskStatus() == MQPCFConstants.TASK_STARTING)
				|| (taskStatus() == MQPCFConstants.TASK_RUNNING)) {
			
			if (taskStatus() == MQPCFConstants.TASK_STARTING) {
				taskStatus(MQPCFConstants.TASK_RUNNING);
			}
			
			if (Thread.interrupted()) {
				taskStatus(MQPCFConstants.TASK_STOPPING);
				
			}
						
			try {		
				MQMessage message = new MQMessage ();

				//message.messageId = MQConstants.MQMI_NONE;
				//message.correlationId = MQConstants.MQMI_NONE;
				getQueue().get (message, getGMO());
	
				if (message.format.equals(MQConstants.MQFMT_EVENT)) {
					calcDayBucket(message.putDateTime);
					
				//	this.cal = message.putDateTime;
				//	zdt = this.cal.toZonedDateTime();
				//	instant = zdt.toInstant();
				//	ZonedDateTime z = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
				//	long timeInMilli = z.toInstant().toEpochMilli();

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
					
					channelName = pcf.getStringParameterValue(MQConstants.MQCACH_CHANNEL_NAME);
					StringBuilder sb = new StringBuilder();
					sb.append(lookupChannelStart + "_");
					sb.append(getHour() + "_");
					sb.append(getDay() + "_");
					sb.append(getMonth() + "_");
					sb.append(getYear() + "_");
					sb.append(eventName + "_");
					sb.append(status + "_");
					sb.append(channelName + "_");
					sb.append(queueName);
					
					String label = sb.toString();

					AtomicLong v = this.ChannelEventsMap.get(label);
					if (v == null) {
						ChannelEventsMap.put(label, 
							meterRegistry.gauge(lookupChannelStart, 
							Tags.of("queueManagerName", getQueueManagerName(),
									"eventName",eventName,
									"status", status,
									"channelName",channelName,
									"hour", Integer.toString(getHour()),
									"day",Integer.toString(getDay()),
									"month",Integer.toString(getMonth()),
									"year",Integer.toString(getYear())
							),
								new AtomicLong(1))
							);
						log.info("Event created ...");
					} else {
						v.incrementAndGet();
						//long x = v.get();
						//v.set(x+1);
					}									
					
					
					/*
					switch (reason) {
					
						case MQConstants.MQRC_CHANNEL_STARTED:
							log.info("Channel Started");
							channelName = pcf.getStringParameterValue(MQConstants.MQCACH_CHANNEL_NAME);

							try {
								xmitQ = pcf.getStringParameterValue(MQConstants.MQCACH_XMIT_Q_NAME).trim();
							
							} catch (Exception e) {
								xmitQ = "";
							}
							
							StringBuilder sb = new StringBuilder();
							sb.append("QueueManager_");
							sb.append(getHour() + "_");
							sb.append(getDay() + "_");
							sb.append(getMonth() + "_");
							sb.append(getYear() + "_");
							sb.append(getMonth() + "_");
							sb.append(MQConstants.MQRC_CHANNEL_STARTED + "_");
							sb.append(channelName + "_");
							sb.append(queueName);
							
							//String label = "QueueManager" + "_" + Long.toString(timeInMilli) + "_" + MQConstants.MQRC_CHANNEL_STARTED + "_" + channelName + "_" + queueName;
							String label = sb.toString();

							//	if (pcf.getParameterCount() == MQConstants.MQCS_SUSPENDED_USER_ACTION) { // Queue manager start
							AtomicLong v = this.ChannelEventsMap.get(label);
							if (v == null) {
								ChannelEventsMap.put(label, 
									meterRegistry.gauge(lookupChannelStart, 
									Tags.of("queueManagerName", getQueueManagerName(),
											"eventName","MQRC_CHANNEL_STARTED",
											"channelName",channelName,
											"hour", Integer.toString(getHour()),
											"day",Integer.toString(getDay()),
											"month",Integer.toString(getMonth()),
											"year",Integer.toString(getYear())
									),
										new AtomicLong(1))
									);
								log.info("Event created ...");
							} else {
								v.incrementAndGet();
								//long x = v.get();
								//v.set(x+1);
							}									
							break;

						case MQConstants.MQRC_CHANNEL_STOPPED:
							log.info("Channel Stopped");
							channelName = pcf.getStringParameterValue(MQConstants.MQCACH_CHANNEL_NAME);
							try {
								xmitQ = pcf.getStringParameterValue(MQConstants.MQCACH_XMIT_Q_NAME).trim();
							
							} catch (Exception e) {
								xmitQ = "";
							}

							sb = new StringBuilder();
							sb.append("QueueManager_");
							sb.append(getHour() + "_");
							sb.append(getDay() + "_");
							sb.append(getMonth() + "_");
							sb.append(getYear() + "_");
							sb.append(getMonth() + "_");
							sb.append(MQConstants.MQRC_CHANNEL_STOPPED + "_");
							sb.append(channelName + "_");
							sb.append(queueName);
							
							//label = "QueueManager" + "_" + Long.toString(timeInMilli) + "_" + MQConstants.MQRC_CHANNEL_STOPPED + "_" + channelName + "_" + queueName;
							label = sb.toString();
							
							AtomicLong vStop = this.ChannelEventsMap.get(label);
							if (vStop == null) {
								ChannelEventsMap.put(label, 
									meterRegistry.gauge(lookupChannelStop, 
									Tags.of("queueManagerName", getQueueManagerName(),
											"eventName","MQRC_CHANNEL_STOPPED",
											"channelName",channelName,
											"hour", Integer.toString(getHour()),
											"day",Integer.toString(getDay()),
											"month",Integer.toString(getMonth()),
											"year",Integer.toString(getYear())

									),
										new AtomicLong(1))
									);
								log.info("Event created ...");
							
							} else {
								vStop.incrementAndGet();
							//	long x = vStop.get();
							//	vStop.set(x+1);
							}
							break;

						case MQConstants.MQRC_CHANNEL_STOPPED_BY_USER:
							log.info("Channel Stopped");
							channelName = pcf.getStringParameterValue(MQConstants.MQCACH_CHANNEL_NAME);
							reason = pcf.getIntParameterValue(MQConstants.MQIACF_REASON_QUALIFIER);
							String status = MQConstants.lookup(reason, "MQRQ_.*").trim();

							sb = new StringBuilder();
							sb.append("QueueManager_");
							sb.append(getHour() + "_");
							sb.append(getDay() + "_");
							sb.append(getMonth() + "_");
							sb.append(getYear() + "_");
							sb.append(getMonth() + "_");
							sb.append(MQConstants.MQRC_CHANNEL_STOPPED_BY_USER + "_");
							sb.append(channelName + "_");
							sb.append(reason + "_");
							sb.append(channelName + "_");
							sb.append(queueName);
							
							//label = "QueueManager" + "_" + Long.toString(timeInMilli) + "_" + MQConstants.MQRC_CHANNEL_STOPPED + "_" + channelName + "_" + queueName;
							label = sb.toString();
							
							AtomicLong vStopByUser = this.ChannelEventsMap.get(label);
							if (vStopByUser == null) {
								ChannelEventsMap.put(label, 
									meterRegistry.gauge(lookupChannelStop, 
									Tags.of("queueManagerName", getQueueManagerName(),
											"eventName","MQRC_CHANNEL_STOPPED_BY_USER",
											"status",status,
											"channelName",channelName,
											"hour", Integer.toString(getHour()),
											"day",Integer.toString(getDay()),
											"month",Integer.toString(getMonth()),
											"year",Integer.toString(getYear())

									),
										new AtomicLong(1))
									);
								log.info("Event created ...");
							
							} else {
								vStopByUser.incrementAndGet();
							//	long x = vStop.get();
							//	vStop.set(x+1);
							}
							
							break;
							
						default:
							String r = MQConstants.lookup(reason, "MQRC_.*");
							log.info("Reason : {}, {}", reason, r);
							
							break;
					}
					*/
					
					//Enumeration<PCFParameter> parms = pcf.getParameters();					
					//int name = pcf.getIntParameterValue(MQConstants.MQIACF_REASON_QUALIFIER);

					//String name = MQConstants.lookup(reason, "MQRQ_.*");

					//final String label = lookupStartAndStopEvents + "_" + channelName + "_" + getQueueManagerName();
					
					//AtomicLong put = startAndStopEventsMap.get(label);
					//if (put == null) {
					//	startAndStopEventsMap.put(label, meterRegistry.gauge(lookupStartAndStopEvents, 
					//			Tags.of("queueManagerName", getQueueManagerName()
					//					),
					//			new AtomicLong(1))
					//			);
					//	log.info("Event created ...");
					//} else {
					//	put.set(1);		
					//}
					
					// 31 - failover not permitted
					int x = 0;
					
				}
				
				deleteMessagesUnderCursor();
				
				if (getBrowse()) {
					getGMO().options = MQConstants.MQGMO_BROWSE_NEXT 
							| MQConstants.MQGMO_NO_WAIT
							| MQConstants.MQGMO_FAIL_IF_QUIESCING
							| MQConstants.MQGMO_CONVERT;
					
				} else {
					getGMO().options = MQConstants.MQGMO_NO_WAIT
							| MQConstants.MQGMO_FAIL_IF_QUIESCING
							| MQConstants.MQGMO_CONVERT
							| MQConstants.MQGMO_SYNCPOINT;
				}
				
								
			} catch (MQException e) {
				if (e.getReason() == MQConstants.MQRC_NO_MSG_AVAILABLE) {
					//log.info("No more channel event messages");
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
					log.info("Unknown property in channel event messages");
					setRetCode(e.getReason());
			}
    		
			
		} // end of while 
		
		taskStatus(MQPCFConstants.TASK_STOPPED);
		setRetCode(0);
		log.info("Channel task stopped");

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
	 * @throws MQException 
	 * 
	 */
    @Override
	public void deleteMessagesUnderCursor() throws MQException {

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
		} else {
			getQueueManager().commit();

		}
		
	}
		
    /**
     * Triggered by the main thread
     * 
     * Set the taskStatus to STOPPING
     * 
     */
    @PreDestroy	
    public void destroy() {
    	log.info("channelevents: destroy");
    	int maxCount = 0;
    	
    	do {
    		taskStatus(MQPCFConstants.TASK_STOPPING);
	    	maxCount++;
	    	
    	} while (taskStatus() == MQPCFConstants.TASK_RUNNING || maxCount > 10);
    	
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
