package maersk.com.mq.configevents;

import java.io.IOException;
import java.net.MalformedURLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.aspectj.apache.bcel.generic.LOOKUPSWITCH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.pcf.PCFMessage;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import maersk.com.mq.events.IQueueManagerEvents;
import maersk.com.mq.events.MQMetricsQueueManager;
import maersk.com.mq.events.MQPCFConstants;

@Component
public class ConfigEvents implements Callable<Integer>,IQueueManagerEvents{

	private final static Logger log = LoggerFactory.getLogger(ConfigEvents.class);

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

    private final String queueName = "SYSTEM.ADMIN.CONFIG.EVENT";
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
    
    private Map<String,AtomicLong>configEventsMap = new HashMap<String,AtomicLong>();
	protected static final String lookupConfigEvents = "mq:configevents";
    
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
	
	
	@Override
    @PostConstruct
    public void init() {
    	log.info("channelevents: init");    	
    	taskStatus(MQPCFConstants.TASK_STOPPED);
    	
    }

	@Override
	public Integer call() throws MQException, MQDataException, IOException, InterruptedException {

    	setQueueManager(getMetricQueueManager().createQueueManager());    	

		setQueueManagerName(getQueueManager().getName().trim());
		setRetCode(0);
		
    	runTask();
    	return getRetCode();
	}

	@Override
	public void runTask() throws MQException, MQDataException, IOException {
		log.info("configevents: run task ");		

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
			log.info("Unable to read messages from {}", getQueueName());
			setRetCode(e.getReason());
			
		} catch (MQDataException e) {
			log.info("Unable to read messages from {}", getQueueName());
			setRetCode(e.getReason());
			
		} catch (IOException e) {
			log.info("Unable to read messages from {}", getQueueName());
			setRetCode(20);
		}
		
	}

	@Override
	public void openQueueForReading() throws MQException {
		log.info("configevents: open queues ");		
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

	@Override
	public void readEventsFromQueue() throws MQException, MQDataException, IOException {
		log.info("configevents: read events ");		
		taskStatus(MQPCFConstants.TASK_STARTING);

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
					
					PCFMessage pcf = new PCFMessage (message);
					int event = pcf.getReason();
					String eventName = MQConstants.lookup(event, "MQRC_.*").trim();
					
					int objectType = 0;
					try {
						objectType = pcf.getIntParameterValue(MQConstants.MQIACF_OBJECT_TYPE);
					
					} catch (Exception e) {
					}

					String objectName = "";				
					switch (objectType) {
					
						case MQConstants.MQOT_NONE:
							break;
							
						case MQConstants.MQOT_CHANNEL:
							objectName = pcf.getStringParameterValue(MQConstants.MQCACH_CHANNEL_NAME).trim();
							break;
							
						case MQConstants.MQOT_NAMELIST:
							objectName = pcf.getStringParameterValue(MQConstants.MQCA_NAMELIST_NAME).trim();
							break;
							
						case MQConstants.MQOT_PROCESS:
							objectName = pcf.getStringParameterValue(MQConstants.MQCA_PROCESS_NAME).trim();
							break;
							
						case MQConstants.MQOT_Q:
							objectName = pcf.getStringParameterValue(MQConstants.MQCA_Q_NAME).trim();
							break;

						case MQConstants.MQOT_Q_MGR:
							objectName = pcf.getStringParameterValue(MQConstants.MQCA_Q_MGR_NAME).trim();
							break;
							
						case MQConstants.MQOT_STORAGE_CLASS:
							objectName = pcf.getStringParameterValue(MQConstants.MQCA_STORAGE_CLASS).trim();
							break;

						case MQConstants.MQOT_AUTH_INFO:
							objectName = pcf.getStringParameterValue(MQConstants.MQCA_AUTH_INFO_NAME).trim();
							break;
							
						case MQConstants.MQOT_CF_STRUC:
							objectName = pcf.getStringParameterValue(MQConstants.MQCA_CF_STRUC_NAME).trim();
							break;

						case MQConstants.MQOT_TOPIC:
							objectName = pcf.getStringParameterValue(MQConstants.MQCA_TOPIC_NAME).trim();
							break;

						case MQConstants.MQOT_COMM_INFO:
							objectName = pcf.getStringParameterValue(MQConstants.MQCA_COMM_INFO_NAME).trim();
							break;

						case MQConstants.MQOT_LISTENER:
							objectName = pcf.getStringParameterValue(MQConstants.MQCACH_LISTENER_NAME).trim();
							break;
							
						default:
							break;
					}
					
					StringBuilder sb = new StringBuilder();
					sb.append(lookupConfigEvents + "_");
					sb.append(getHour() + "_");
					sb.append(getDay() + "_");
					sb.append(getMonth() + "_");
					sb.append(getYear() + "_");
					sb.append(eventName + "_");
					sb.append(objectType + "_");
					
					String label = sb.toString();

					AtomicLong v = this.configEventsMap.get(label);
					if (v == null) {
						configEventsMap.put(label, 
							meterRegistry.gauge(lookupConfigEvents, 
							Tags.of("queueManagerName", getQueueManagerName(),
									"eventName",eventName,
									"objectType", MQConstants.lookup(objectType, "MQOT_.*").trim(),
									"objectName",objectName,
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
					}									
					
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
					//log.info("No more config event messages");
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
					log.info("Unknown property in config event messages");
					setRetCode(e.getReason());
			}
    		
			
		} // end of while 
		
		taskStatus(MQPCFConstants.TASK_STOPPED);
		setRetCode(0);
		log.info("Config task stopped");

    }


	private void calcDayBucket(GregorianCalendar putDateTime) {
		ZonedDateTime zdt;
		Instant instant;

		this.cal = putDateTime;
		zdt = this.cal.toZonedDateTime();
		instant = zdt.toInstant();
		ZonedDateTime z = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);

		setHour(this.cal.get(Calendar.HOUR_OF_DAY)); 
		setDay(this.cal.get(Calendar.DAY_OF_MONTH));
		//int weekOfYear = this.cal.get(Calendar.WEEK_OF_YEAR);
		setMonth((this.cal.get(Calendar.MONTH) + 1)); // Month is indexed from 0 !!, so, JAN = 0, FEB = 1 etc 
		setYear(this.cal.get(Calendar.YEAR));
				
	}
	
	@Override
	public void deleteMessagesUnderCursor() {
		log.info("configevents: delete messages ");		
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

	@Override
	@PreDestroy
	public void destroy()  {
		log.info("configevents: destroy ");		

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
