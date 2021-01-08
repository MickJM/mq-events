package app.com.mq.configevents;

/*
 * Copyright 2020
 * Maersk
 *
 * Get MQ Config events
 * 
 * 01/07/2020 - Get MQ Configuration events
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
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.aspectj.apache.bcel.generic.LOOKUPSWITCH;
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
import com.ibm.mq.headers.pcf.PCFContent;
import com.ibm.mq.headers.pcf.PCFException;
import com.ibm.mq.headers.pcf.PCFMessage;
import com.ibm.mq.headers.pcf.PCFParameter;

import app.com.mq.events.IEvents;
import app.com.mq.events.IMQPCFConstants;
import app.com.mq.events.MQMetricsQueueManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

@Component
public class ConfigEvents implements Callable<Integer>,IEvents, IMQPCFConstants {

	private final static Logger log = LoggerFactory.getLogger(ConfigEvents.class);

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
	
	private String queuemanagername;
	public void QueueManagerName(String v) {
		this.queuemanagername = v;
	}
	public String QueueManagerName() {
		return this.queuemanagername;
	}

    @Value("${ibm.mq.event.config.queue:SYSTEM.ADMIN.CONFIG.EVENT}")
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
    
    private Map<String,AtomicLong>configEventsMap = new HashMap<String,AtomicLong>();
	protected static final String lookupConfigQueueEvents = "mq:queue_events";
	protected static final String lookupConfigQueueManagerEvents = "mq:queuemanager_events";
	protected static final String lookupConfigChannelEvents = "mq:channel_events";
	protected static final String lookupConfigAuthInfoEvents = "mq:authinfo_events";
	    
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
	private String timeStamp;
	private void setZonedTime(String v) {
		this.timeStamp = v;
	}
	private String getZonedTime() {
		return this.timeStamp;
	}
	
	private MQMessage message;
	private PCFMessage pcfmessage;
	public void PCFMessage(PCFMessage v) {
		this.pcfmessage = v;
	}
	public PCFMessage PCFMessage() {
		return this.pcfmessage;
	}
	
	private PCFMessage afterpcfmessage;
	public PCFMessage AfterPCFMessage() {
		return this.afterpcfmessage;
	}
	public void AfterPCFMessage(PCFMessage v) {
		this.afterpcfmessage = v;
	}
	
	@Override
    @PostConstruct
    public void Init() {
    	log.info("configevents: Init");    	
    	TaskStatus(IMQPCFConstants.TASK_STOPPED);
    	
    }

	@Override
	public Integer call() throws MQException, MQDataException, IOException, InterruptedException {

    	QueueManager(MetricsQueueManager().CreateQueueManager());    	
		QueueManagerName(QueueManager().getName().trim());
		RetCode(IMQPCFConstants.OKAY);
		
    	RunTask();
    	return RetCode();
	}

	@Override
	public void RunTask() throws MQException, MQDataException, IOException {
		log.debug("configevents: RunTask ");		

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
			log.info("Unable to read messages from {}", QueueName());
			RetCode(e.getReason());
			
		} catch (MQDataException e) {
			log.info("Unable to read messages from {}", QueueName());
			RetCode(e.getReason());
			
		} catch (IOException e) {
			log.info("Unable to read messages from {}", QueueName());
			RetCode(IMQPCFConstants.RET_WITH_ERROR);
		}
		
	}

	@Override
	public void OpenQueueForReading() throws MQException {
		log.info("configevents: OpenQueueForReading ");		

		int openOptions = MQConstants.MQOO_INPUT_AS_Q_DEF |
				MQConstants.MQOO_BROWSE |
				MQConstants.MQOO_FAIL_IF_QUIESCING;			

		GetMessageOptions(new MQGetMessageOptions());		
		Queue(QueueManager().accessQueue(queueName, openOptions));

		SetGMOOptions(MQConstants.MQGMO_BROWSE_FIRST);
		
	}

	@Override
	public void ReadEventsFromQueue() throws MQException, MQDataException, IOException {
		log.debug("Read events ");		
		TaskStatus(IMQPCFConstants.TASK_STARTING);

		while ((TaskStatus() == IMQPCFConstants.TASK_STARTING)
				|| (TaskStatus() == IMQPCFConstants.TASK_RUNNING)) {
			
			if (TaskStatus() == IMQPCFConstants.TASK_STARTING) {
				TaskStatus(IMQPCFConstants.TASK_RUNNING);
			}			
			if (Thread.interrupted()) {
				TaskStatus(IMQPCFConstants.TASK_STOPPING);
				
			}
						
			/*
			 * message sequence = 1 (NOT_LAST) - before update
			 * message sequence = 2 (LAST)     - altered value
			 * 
			 */
			byte[] correl;
			
			try {		
				this.message = new MQMessage();
				Queue().get (this.message, GetMessageOptions());				
				if (this.message.format.equals(MQConstants.MQFMT_EVENT)) {
					
					PCFMessage(new PCFMessage (this.message));
					int event = PCFMessage().getReason();
					
					int objectType = 0;
					try {
						objectType = PCFMessage().getIntParameterValue(MQConstants.MQIACF_OBJECT_TYPE);
					
					} catch (Exception e) {
					}

					String objectName = "";
					String userId = "";
					int maxQueueDepth = 0;
					
					switch (objectType) {
					
						case MQConstants.MQOT_NONE:
							break;
							
						case MQConstants.MQOT_CHANNEL:
							//objectName = PCFMessage().getStringParameterValue(MQConstants.MQCACH_CHANNEL_NAME).trim();
							ChannelEvents();
							break;
							
						case MQConstants.MQOT_NAMELIST:
							objectName = PCFMessage().getStringParameterValue(MQConstants.MQCA_NAMELIST_NAME).trim();
							break;
							
						case MQConstants.MQOT_PROCESS:
							objectName = PCFMessage().getStringParameterValue(MQConstants.MQCA_PROCESS_NAME).trim();
							break;
							
						case MQConstants.MQOT_Q:							
							QueueEvents();							
							break;

						case MQConstants.MQOT_Q_MGR:
							QueueManagerEvents();
							break;
							
						case MQConstants.MQOT_STORAGE_CLASS:
							objectName = PCFMessage().getStringParameterValue(MQConstants.MQCA_STORAGE_CLASS).trim();
							break;

						case MQConstants.MQOT_AUTH_INFO:
							objectName = PCFMessage().getStringParameterValue(MQConstants.MQCA_AUTH_INFO_NAME).trim();
							AuthInfoEvents();							
							break;
							
						case MQConstants.MQOT_CF_STRUC:
							objectName = PCFMessage().getStringParameterValue(MQConstants.MQCA_CF_STRUC_NAME).trim();
							break;

						case MQConstants.MQOT_TOPIC:
							objectName = PCFMessage().getStringParameterValue(MQConstants.MQCA_TOPIC_NAME).trim();
							break;

						case MQConstants.MQOT_COMM_INFO:
							objectName = PCFMessage().getStringParameterValue(MQConstants.MQCA_COMM_INFO_NAME).trim();
							break;

						case MQConstants.MQOT_LISTENER:
							objectName = PCFMessage().getStringParameterValue(MQConstants.MQCACH_LISTENER_NAME).trim();
							break;
							
						default:
							break;
					}
					
					// each metric is called from above
				}
				
				DeleteMessagesUnderCursor();
				SetGMOOptions(MQConstants.MQGMO_BROWSE_NEXT);
								
			} catch (MQException e) {
				if (e.getReason() == MQConstants.MQRC_NO_MSG_AVAILABLE) {
					log.debug("No more config event messages");
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
					log.info("Unknown property in config event messages");
					RetCode(e.getReason());
			}
    		
			
		} // end of while 
		
		TaskStatus(IMQPCFConstants.TASK_STOPPED);
		RetCode(IMQPCFConstants.OKAY);
		log.info("Config task stopped");

    }

	/*
	 * Process Channel Events
	 */
	private void ChannelEvents() throws MQException, MQDataException, IOException {
		
		if (PCFMessage().getReason() == MQConstants.MQRC_CONFIG_CHANGE_OBJECT) {
			log.debug("Channel change event");
			String beforeafter = "";
			if (PCFMessage().getMsgSeqNumber() == IMQPCFConstants.SEQ_FIRST) {
				beforeafter = "before";
				DeleteMessagesUnderCursor();
				SetGMOOptions(MQConstants.MQGMO_BROWSE_NEXT);

				MQMessage aftermsg = new MQMessage ();
				aftermsg.correlationId = message.correlationId;			// Each pair of change events, has the same correlId
				Queue().get (aftermsg, GetMessageOptions());
				AfterPCFMessage(new PCFMessage(aftermsg));
				
				if (AfterPCFMessage().getMsgSeqNumber() == IMQPCFConstants.SEQ_SECOND) {
					CompareChannelMessages();
					
				} else {
					log.warn("Channel change events are out of sequence, continuing, but metics may be wrong");
				}	
			}
		} else if (PCFMessage().getReason() == MQConstants.MQRC_CONFIG_CREATE_OBJECT) {
		//	log.info("Channel event - create object not yet implmented");
			AfterPCFMessage(null);
			CompareChannelMessages();
			
		} else if (PCFMessage().getReason() == MQConstants.MQRC_CONFIG_DELETE_OBJECT) {
			log.info("Channel event - delete object not yet implmented");		
			AfterPCFMessage(null);
			CompareChannelMessages();
			
		} else if (PCFMessage().getReason() == MQConstants.MQRC_CONFIG_REFRESH_OBJECT) {
			log.info("Channel event - refresh object not yet implmented");
			
		} else {
			log.info("Channel event unknown: {}", PCFMessage().getReason());
			
		}

	}

	/*
	 * Compare each channel message
	 */
	@SuppressWarnings("unchecked")
	private void CompareChannelMessages() throws MQDataException, IOException {
		
		CalcDayBucket(message.putDateTime);

		if (this.message.format.equals(MQConstants.MQFMT_EVENT)) {
			Enumeration<PCFParameter> bparms = PCFMessage().getParameters();
			Enumeration<PCFParameter> aparms = null;
			if (AfterPCFMessage() != null) {
				aparms = AfterPCFMessage().getParameters();					
			}
			while (bparms.hasMoreElements()) {
				
				PCFParameter bpcfParams = bparms.nextElement();
				PCFParameter apcfParams = null;
				if (aparms != null) {
					apcfParams = aparms.nextElement();
				}
				switch (bpcfParams.getParameter()) {
				
					default:						
						String first = bpcfParams.getStringValue();
						String second = null;
						if (apcfParams != null) {
							second = apcfParams.getStringValue();
						}
			
						if (!first.equals(second)) {
							String channelName = PCFMessage().getStringParameterValue(MQConstants.MQCACH_CHANNEL_NAME).trim();
							
							int bobjectType = PCFMessage().getIntParameterValue(MQConstants.MQIACF_OBJECT_TYPE);		
							String bobjectName = PCFMessage().getStringParameterValue(MQConstants.MQCACF_EVENT_Q_MGR).trim();
							String beventName = MQConstants.lookup(PCFMessage().getReason(), "MQRC_.*").trim();
							String buserId = PCFMessage().getStringParameterValue(MQConstants.MQCACF_EVENT_USER_ID).trim();

							int aobjectType = 0;
							String aobjectName = null;
							String aeventName = null;
							String auserId = null;
							if (AfterPCFMessage() != null) {
								aobjectType = AfterPCFMessage().getIntParameterValue(MQConstants.MQIACF_OBJECT_TYPE);		
								aobjectName = AfterPCFMessage().getStringParameterValue(MQConstants.MQCACF_EVENT_Q_MGR).trim();
								aeventName = MQConstants.lookup(AfterPCFMessage().getReason(), "MQRC_.*").trim();
								auserId = AfterPCFMessage().getStringParameterValue(MQConstants.MQCACF_EVENT_USER_ID).trim();								
							}
							
							ChannelChanges(channelName, bpcfParams, beventName, bobjectType, 
									buserId, "reserved", first, IMQPCFConstants.QE_BEFORE);
							if (AfterPCFMessage() != null) { 							
								ChannelChanges(channelName, apcfParams, aeventName, aobjectType, 
										auserId, "reserved", second, IMQPCFConstants.QE_AFTER);			
							}
						}			
						break;
				}
			}
		}
	}
		
	/*
	 * Process queue manager event messages
	 */
	private void QueueManagerEvents() throws MQException, MQDataException, IOException {

		// https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_7.5.0/com.ibm.mq.ref.adm.doc/q087000_.htm
		
		if (PCFMessage().getReason() == MQConstants.MQRC_CONFIG_CHANGE_OBJECT) {
			String beforeafter = "";
			if (PCFMessage().getMsgSeqNumber() == IMQPCFConstants.SEQ_FIRST) {
				beforeafter = "before";					
				DeleteMessagesUnderCursor();
				SetGMOOptions(MQConstants.MQGMO_BROWSE_NEXT);

				MQMessage aftermsg = new MQMessage ();
				aftermsg.correlationId = message.correlationId;			// Each pair of change events, has the same correlId
				Queue().get (aftermsg, GetMessageOptions());
				AfterPCFMessage(new PCFMessage(aftermsg));

				if (AfterPCFMessage().getMsgSeqNumber() == IMQPCFConstants.SEQ_SECOND) {
					CompareQueueManagerMessages();
					
				} else {
					log.warn("Queue Manager change events are out of sequence, continuing, but metics may be wrong");
				}
				
			}

		} else {
			log.warn("Queue Manager events found for " + PCFMessage().getReason());
			log.warn("Continuing processing");
		}
	}
		
	/*
	 * Compare each queue manager events
	 */
	@SuppressWarnings("unchecked")
	private void CompareQueueManagerMessages() throws PCFException {
		
		CalcDayBucket(this.message.putDateTime);
			
		if (this.message.format.equals(MQConstants.MQFMT_EVENT)) {
			Enumeration<PCFParameter> bparms = PCFMessage().getParameters();					
			Enumeration<PCFParameter> aparms = AfterPCFMessage().getParameters();					

			while (bparms.hasMoreElements()) {				
				PCFParameter bpcfParams = bparms.nextElement();
				PCFParameter apcfParams = aparms.nextElement();
				
				switch (bpcfParams.getParameter()) {
				
					default:
						
						String first = bpcfParams.getStringValue();
						String second = apcfParams.getStringValue();
						
						if (!first.equals(second)) {
							int bobjectType = PCFMessage().getIntParameterValue(MQConstants.MQIACF_OBJECT_TYPE);		
							int aobjectType = AfterPCFMessage().getIntParameterValue(MQConstants.MQIACF_OBJECT_TYPE);		
							
							String bobjectName = PCFMessage().getStringParameterValue(MQConstants.MQCA_Q_MGR_NAME).trim();
							String aobjectName = AfterPCFMessage().getStringParameterValue(MQConstants.MQCA_Q_MGR_NAME).trim();

							String beventName = MQConstants.lookup(PCFMessage().getReason(), "MQRC_.*").trim();
							String aeventName = MQConstants.lookup(AfterPCFMessage().getReason(), "MQRC_.*").trim();
							
							String buserId = PCFMessage().getStringParameterValue(MQConstants.MQCACF_EVENT_USER_ID).trim();
							String auserId = AfterPCFMessage().getStringParameterValue(MQConstants.MQCACF_EVENT_USER_ID).trim();

							QueueManagerChanges(bpcfParams, beventName, bobjectType, 
									buserId, "reserved", first, IMQPCFConstants.QE_BEFORE);
							QueueManagerChanges(apcfParams, aeventName, aobjectType, 
									auserId, "reserved", second, IMQPCFConstants.QE_AFTER);			
														
						}
						break;
				
				}
			}
		}
		
		log.debug("Queue Manager events");
	}
	
	/*
	 * Queue Events
	 */
	private void QueueEvents() throws MQException, MQDataException, IOException {

		// https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_9.1.0/com.ibm.mq.mon.doc/q036480_.htm
		
		if (PCFMessage().getReason() == MQConstants.MQRC_CONFIG_CHANGE_OBJECT) {
			log.debug("Queue Event: Change Object");

			String beforeafter = "";
			if (PCFMessage().getMsgSeqNumber() == IMQPCFConstants.SEQ_FIRST) {
				beforeafter = "before";					
				DeleteMessagesUnderCursor();
				SetGMOOptions(MQConstants.MQGMO_BROWSE_NEXT);

				MQMessage aftermsg = new MQMessage ();
				aftermsg.correlationId = message.correlationId;			// Each pair of change events, has the same correlId
				Queue().get (aftermsg, GetMessageOptions());
				AfterPCFMessage(new PCFMessage(aftermsg));

				if (AfterPCFMessage().getMsgSeqNumber() == IMQPCFConstants.SEQ_SECOND) {
					CompareQueueMessages();
					
				} else {
					log.info("Change events are out of sequence, continuing, but metics may be wrong");
				}
				
			}

		} else if (PCFMessage().getReason() == MQConstants.MQRC_CONFIG_CREATE_OBJECT) {
			log.debug("Queue Event: Create Object");
			
			int objectType = PCFMessage().getIntParameterValue(MQConstants.MQIACF_OBJECT_TYPE);
			String eventName = MQConstants.lookup(PCFMessage().getReason(), "MQRC_.*").trim();
			String userId = PCFMessage().getStringParameterValue(MQConstants.MQCACF_EVENT_USER_ID).trim();
			String objectName = PCFMessage().getStringParameterValue(MQConstants.MQCA_Q_NAME).trim();
			QueueMetric(eventName, objectType, userId, objectName);

		} else if (PCFMessage().getReason() == MQConstants.MQRC_CONFIG_DELETE_OBJECT) {
			log.debug("Queue Event: Delete Object");
			
			int objectType = PCFMessage().getIntParameterValue(MQConstants.MQIACF_OBJECT_TYPE);
			String eventName = MQConstants.lookup(PCFMessage().getReason(), "MQRC_.*").trim();
			String userId = PCFMessage().getStringParameterValue(MQConstants.MQCACF_EVENT_USER_ID).trim();
			String objectName = PCFMessage().getStringParameterValue(MQConstants.MQCA_Q_NAME).trim();
			QueueMetric(eventName, objectType, userId, objectName);
			
		} else if (PCFMessage().getReason() == MQConstants.MQRC_CONFIG_REFRESH_OBJECT) {
			log.debug("Queue Event: Refresh Object");
			
			int objectType = PCFMessage().getIntParameterValue(MQConstants.MQIACF_OBJECT_TYPE);
			String eventName = MQConstants.lookup(PCFMessage().getReason(), "MQRC_.*").trim();
			String userId = PCFMessage().getStringParameterValue(MQConstants.MQCACF_EVENT_USER_ID).trim();
			String objectName = PCFMessage().getStringParameterValue(MQConstants.MQCA_Q_NAME).trim();
			QueueMetric(eventName, objectType, userId, objectName);
			
		} else {
			log.warn("Queue Event: Unknown event - " + PCFMessage().getReason());

		}
	}
	
	private void AuthInfoEvents() throws MQException, MQDataException, IOException {

		if (PCFMessage().getReason() == MQConstants.MQRC_CONFIG_CHANGE_OBJECT) {
			String beforeafter = "";
			if (PCFMessage().getMsgSeqNumber() == IMQPCFConstants.SEQ_FIRST) {
				beforeafter = "before";	
				DeleteMessagesUnderCursor();
				SetGMOOptions(MQConstants.MQGMO_BROWSE_NEXT);

				MQMessage aftermsg = new MQMessage ();
				aftermsg.correlationId = message.correlationId;			// Each pair of change events, has the same correlId
				Queue().get (aftermsg, GetMessageOptions());
				AfterPCFMessage(new PCFMessage(aftermsg));

				if (AfterPCFMessage().getMsgSeqNumber() == IMQPCFConstants.SEQ_SECOND) {
					CompareAuthInfoMessages();
					log.debug("Processing Authinfo records");
					
				} else {
					log.warn("Change events are out of sequence, continuing, but metics may be wrong");
				}
				
			}
			
		} else if (PCFMessage().getReason() == MQConstants.MQRC_CONFIG_CREATE_OBJECT) {
			log.info("Authinfo event - create object not yet implmented");
		
		} else if (PCFMessage().getReason() == MQConstants.MQRC_CONFIG_DELETE_OBJECT) {
			log.info("Authinfo event - delete not yet implemented");
			
		} else if (PCFMessage().getReason() == MQConstants.MQRC_CONFIG_REFRESH_OBJECT) {
			log.info("Authinfo event - refresh not yet implemented");
			
		} else {
			log.info("Authinfo event unknown object");	
		}
	}

	@SuppressWarnings("unchecked")
	private void CompareAuthInfoMessages() throws PCFException {

		log.debug("Comparing before and after messages ...");
		
		CalcDayBucket(this.message.putDateTime);
		
		/*
		 * Loop through each PCF message and compare record1 with record2
		 */
		if (this.message.format.equals(MQConstants.MQFMT_EVENT)) {
			Enumeration<PCFParameter> bparms = PCFMessage().getParameters();					
			Enumeration<PCFParameter> aparms = AfterPCFMessage().getParameters();					

			while (bparms.hasMoreElements()) {
				PCFParameter bpcfParams = bparms.nextElement();
				PCFParameter apcfParams = aparms.nextElement();
				
				switch (bpcfParams.getParameter()) {
				
					default:
						
						String first = bpcfParams.getStringValue();
						String second = apcfParams.getStringValue();
						
						if (!first.equals(second)) {
							int bobjectType = PCFMessage().getIntParameterValue(MQConstants.MQIACF_OBJECT_TYPE);
							int aobjectType = AfterPCFMessage().getIntParameterValue(MQConstants.MQIACF_OBJECT_TYPE);

							String bobjectName = PCFMessage().getStringParameterValue(MQConstants.MQCA_AUTH_INFO_NAME).trim();
							String aobjectName = AfterPCFMessage().getStringParameterValue(MQConstants.MQCA_AUTH_INFO_NAME).trim();

							String beventName = MQConstants.lookup(PCFMessage().getReason(), "MQRC_.*").trim();
							String aeventName = MQConstants.lookup(AfterPCFMessage().getReason(), "MQRC_.*").trim();

							String buserId = PCFMessage().getStringParameterValue(MQConstants.MQCACF_EVENT_USER_ID).trim();
							String auserId = AfterPCFMessage().getStringParameterValue(MQConstants.MQCACF_EVENT_USER_ID).trim();
									
							AuthInfoChanges(bpcfParams, beventName, bobjectType, 
									buserId, bobjectName, first, IMQPCFConstants.QE_BEFORE);
							AuthInfoChanges(apcfParams, aeventName, aobjectType, 
									auserId, aobjectName, second, IMQPCFConstants.QE_AFTER);
							
						}
						break;
				}
			}
		}		
	}
	
	
	/*
	 * Compare each queue messages
	 */
	@SuppressWarnings("unchecked")
	private void CompareQueueMessages() throws PCFException {

		log.debug("Comparing before and after messages ...");
		
		CalcDayBucket(this.message.putDateTime);
		
		/*
		 * Loop through each PCF message and compare record1 with record2
		 */
		if (this.message.format.equals(MQConstants.MQFMT_EVENT)) {
			Enumeration<PCFParameter> bparms = PCFMessage().getParameters();					
			Enumeration<PCFParameter> aparms = AfterPCFMessage().getParameters();					

			while (bparms.hasMoreElements()) {
				PCFParameter bpcfParams = bparms.nextElement();
				PCFParameter apcfParams = aparms.nextElement();
				
				switch (bpcfParams.getParameter()) {
				
					default:
						
						String first = bpcfParams.getStringValue();
						String second = apcfParams.getStringValue();
						
						if (!first.equals(second)) {
							int bobjectType = PCFMessage().getIntParameterValue(MQConstants.MQIACF_OBJECT_TYPE);
							int aobjectType = AfterPCFMessage().getIntParameterValue(MQConstants.MQIACF_OBJECT_TYPE);

							String bobjectName = PCFMessage().getStringParameterValue(MQConstants.MQCA_Q_NAME).trim();
							String aobjectName = AfterPCFMessage().getStringParameterValue(MQConstants.MQCA_Q_NAME).trim();

							String beventName = MQConstants.lookup(PCFMessage().getReason(), "MQRC_.*").trim();
							String aeventName = MQConstants.lookup(AfterPCFMessage().getReason(), "MQRC_.*").trim();

							String buserId = PCFMessage().getStringParameterValue(MQConstants.MQCACF_EVENT_USER_ID).trim();
							String auserId = AfterPCFMessage().getStringParameterValue(MQConstants.MQCACF_EVENT_USER_ID).trim();
									
							QueueChanges(bpcfParams, beventName, bobjectType, 
									buserId, bobjectName, first, IMQPCFConstants.QE_BEFORE);
							QueueChanges(apcfParams, aeventName, aobjectType, 
									auserId, aobjectName, second, IMQPCFConstants.QE_AFTER);
							
						}
						break;
				}
			}
		}		
	}
	
	/*
	 * New, Delete and Refresh records
	 */
	private void QueueMetric(String eventName, int objectType, 
								String userId, String objectName) {

		CalcDayBucket(this.message.putDateTime);

		String changedType = ChangeType();
		
		/*
		String changedType = "";
		switch (PCFMessage().getReason()) {
			case MQConstants.MQRC_CONFIG_CREATE_OBJECT:
				changedType = "create";
				break;
				
			case MQConstants.MQRC_CONFIG_DELETE_OBJECT:
				changedType = "delete";
				break;
				
			case MQConstants.MQRC_CONFIG_REFRESH_OBJECT:
				changedType = "refresh";
				break;
				
			default:
				changedType = "unknwon";
				break;
		}
		*/
		
		//String beventName = MQConstants.lookup(PCFMessage().getReason(), "MQRC_.*").trim();

		StringBuilder sb = new StringBuilder();
		sb.append(lookupConfigQueueEvents + "_" + changedType + "_");
		sb.append(eventName + "_");
		sb.append(objectType + "_");
	//	sb.append(objectName + "_");		
		sb.append(getZonedTime());
		
		String label = sb.toString();

		AtomicLong v = this.configEventsMap.get(label);
		if (v == null) {
			this.configEventsMap.put(label, 
				meterRegistry.gauge(lookupConfigQueueEvents + "_" + changedType, 
				Tags.of("queueManagerName", QueueManagerName(),
						"eventName",eventName,
						"objectType", MQConstants.lookup(objectType, "MQOT_.*").trim(),
			//			"objectName",objectName,
						"user",userId,
						"timeStamp",getZonedTime()
				),
					new AtomicLong(1))
				);
			log.debug("Event created ...");
			
		} else {
			v.incrementAndGet();
		}									
	}
	
	/*
	 * Queue changes
	 */
	private void QueueChanges(PCFParameter bpcfParams, String eventName, int objectType, 
				String userId, String objectName, Object value, String beforeafter) {
		
		String changedType = ChangeType();

		/*
		try {
			changedType = MQConstants.lookup(bpcfParams.getParameter(), "MQCA_.*").trim();
		
		} catch (Exception e) {
			try {
				changedType = MQConstants.lookup(bpcfParams.getParameter(), "MQIA_.*").trim();
				
			} catch (Exception e1) {
				changedType = "unknown";
				
			}			
		}
		*/
		
		String attribName = AttributeName(bpcfParams);
		
		StringBuilder sb = new StringBuilder();
		sb.append(lookupConfigQueueEvents + "_" + changedType.trim() + "_");
		sb.append(eventName + "_");
		sb.append(objectType + "_");
		sb.append(value + "_");
		sb.append(getZonedTime() + "_");
		sb.append(beforeafter);
		
		String label = sb.toString();

		String attv = "";
		if (value instanceof Integer) {
			attv = Integer.toString((Integer)value);
			
		} else if (value instanceof String) {
			attv = (String) value;
		}

		AtomicLong v = this.configEventsMap.get(label);
		if (v == null) {
			this.configEventsMap.put(label, 
				meterRegistry.gauge(lookupConfigQueueEvents + "_" + changedType.trim(), 
				Tags.of("queueManagerName", QueueManagerName(),
						"eventName",eventName,
						"objectType", MQConstants.lookup(objectType, "MQOT_.*").trim(),
			//			"objectName",objectName.trim(),
						"user",userId,
						"attributeValue", attv.trim(),
						"attributeName", attribName,
						"timeStamp",getZonedTime(),
						"eventType", beforeafter
				),
					new AtomicLong(1))
				);
			log.debug("Event created ...");
			
		} else {
			v.incrementAndGet();
		}											
	}

	/*
	 * Queue manager changes
	 */
	private void QueueManagerChanges(PCFParameter bpcfParams, String eventName, int objectType, 
				String userId, String reserved, Object value , String beforeafter) {
		
		String changedType = ChangeType();

		/*
		try {
			changedType = MQConstants.lookup(bpcfParams.getParameter(), "MQCA_.*").trim();
		
		} catch (Exception e) {
			try {
				changedType = MQConstants.lookup(bpcfParams.getParameter(), "MQIA_.*").trim();
			} catch (Exception e1) {
				changedType = "unknwon";
				
			}			
		}
		*/
		
		String attribName = AttributeName(bpcfParams);
		
		StringBuilder sb = new StringBuilder();
		sb.append(lookupConfigQueueManagerEvents + "_" + changedType + "_");
		sb.append(eventName + "_");
		sb.append(objectType + "_");
		sb.append(value + "_");
		sb.append(getZonedTime() + "_");
		sb.append(beforeafter);
		String label = sb.toString();

		String attv = "";
		if (value instanceof Integer) {
			attv = Integer.toString((Integer)value);
			
		} else if (value instanceof String) {
			attv = (String) value;
		}
		
		AtomicLong v = this.configEventsMap.get(label);
		if (v == null) {
			this.configEventsMap.put(label, 
				meterRegistry.gauge(lookupConfigQueueManagerEvents + "_" + changedType, 
				Tags.of("queueManagerName", QueueManagerName(),
						"eventName",eventName,
						"objectType", MQConstants.lookup(objectType, "MQOT_.*").trim(),
			//			"objectName",changedType.toUpperCase(),
						"user",userId,
						"attributeValue", attv.trim(),
						"attributeName", attribName,
						"timeStamp",getZonedTime(),
						"eventType", beforeafter
				),
					new AtomicLong(1))
				);
			log.debug("Event created ...");
			
		} else {
			v.incrementAndGet();
		}											
	}
	
	/*
	 * Channel changes
	 */
	private void ChannelChanges(String channelName, PCFParameter bpcfParams, String eventName, int objectType, 
				String userId, String reserved, Object value , String beforeafter) {

		String changedType = ChangeType();

		/*
		String changedType = "";
		switch (PCFMessage().getReason()) {
			case MQConstants.MQRC_CONFIG_CREATE_OBJECT:
				changedType = "create";
				break;
				
			case MQConstants.MQRC_CONFIG_DELETE_OBJECT:
				changedType = "delete";
				break;
				
			case MQConstants.MQRC_CONFIG_REFRESH_OBJECT:
				changedType = "refresh";
				break;
				
			default:
				changedType = "unknwon";
				break;
		}
		*/
		String attribName = AttributeName(bpcfParams);
		
		StringBuilder sb = new StringBuilder();
		sb.append(lookupConfigChannelEvents + "_" + channelName + "_" + changedType + "_");
		sb.append(eventName + "_" + attribName + "_");
		sb.append(objectType + "_");
		sb.append(value + "_");
		sb.append(getZonedTime() + "_");
		sb.append(beforeafter);
		String label = sb.toString();

		String attv = "";
		if (value instanceof Integer) {
			attv = Integer.toString((Integer)value);
			
		} else if (value instanceof String) {
			attv = (String) value;
		}
		
		AtomicLong v = this.configEventsMap.get(label);
		if (v == null) {
			this.configEventsMap.put(label, 
				meterRegistry.gauge(lookupConfigChannelEvents + "_" + channelName + "_" + changedType, 
				Tags.of("queueManagerName", QueueManagerName(),
						"eventName",eventName,
						"objectType", MQConstants.lookup(objectType, "MQOT_.*").trim(),
			//			"objectName",changedType.toUpperCase(),
						"user",userId,
						"attributeValue", attv.trim(),
						"attributeName", attribName.trim(),
						"timeStamp",getZonedTime(),
						"eventType", beforeafter
				),
					new AtomicLong(1))
				);
			log.debug("Event created ...");
			
		} else {
			v.incrementAndGet();
		}											
	}
	
	/*
	 * AuthInfo changes
	 */
	private void AuthInfoChanges(PCFParameter bpcfParams, String eventName, int objectType, 
				String userId, String objectName, Object value, String beforeafter) {
		
		String changedType = ChangeType();
		
		/*
		String changedType = "";
		try {
			changedType = MQConstants.lookup(bpcfParams.getParameter(), "MQCA_.*").trim();
		
		} catch (Exception e) {
			try {
				changedType = MQConstants.lookup(bpcfParams.getParameter(), "MQIA_.*").trim();
				
			} catch (Exception e1) {
				changedType = "unknwon";
				
			}			
		}
		*/
		
		String attribName = AttributeName(bpcfParams);
		
		StringBuilder sb = new StringBuilder();
		sb.append(lookupConfigAuthInfoEvents + "_" + changedType.trim() + "_");
		sb.append(eventName + "_");
		sb.append(objectType + "_");
		sb.append(value + "_");
		sb.append(getZonedTime() + "_");
		sb.append(beforeafter);
		
		String label = sb.toString();

		String attv = "";
		if (value instanceof Integer) {
			attv = Integer.toString((Integer)value);
			
		} else if (value instanceof String) {
			attv = (String) value;
		}

		AtomicLong v = this.configEventsMap.get(label);
		if (v == null) {
			this.configEventsMap.put(label, 
				meterRegistry.gauge(lookupConfigAuthInfoEvents + "_" + changedType.trim(), 
				Tags.of("queueManagerName", QueueManagerName(),
						"eventName",eventName,
						"objectType", MQConstants.lookup(objectType, "MQOT_.*").trim(),
			//			"objectName",objectName.trim(),
						"user",userId,
						"attributeValue", attv.trim(),
						"attributeName", attribName.trim(),
						"timeStamp",getZonedTime(),
						"eventType", beforeafter
				),
					new AtomicLong(1))
				);
			log.debug("Event created ...");
			
		} else {
			v.incrementAndGet();
		}											
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
     * Calculate the correct 'bucket'
     */
	private void CalcDayBucket(GregorianCalendar putDateTime) {
		
		final ZonedDateTime zdt;
		final Instant instant;
		final Calendar cal1 = Calendar.getInstance();
		
		this.cal = putDateTime;
		zdt = this.cal.toZonedDateTime();
		instant = zdt.toInstant();
		ZonedDateTime z = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);

		setZonedTime(z.toString());
		
		Hour(this.cal.get(Calendar.HOUR_OF_DAY)); 
		Day(this.cal.get(Calendar.DAY_OF_MONTH));
		Month((this.cal.get(Calendar.MONTH) + 1)); // Month is indexed from 0 !!, so, JAN = 0, FEB = 1 etc 
		Year(this.cal.get(Calendar.YEAR));
				
	}
	
	/*
	 * Delete the message if needed
	 */
	@Override
	public void DeleteMessagesUnderCursor() {
		log.debug("Delete messages ");		
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

	private String ChangeType() {

		String changeType = "";
		switch (PCFMessage().getReason()) {
			case MQConstants.MQRC_CONFIG_CREATE_OBJECT:
				changeType = "create";
				break;
				
			case MQConstants.MQRC_CONFIG_DELETE_OBJECT:
				changeType = "delete";
				break;
				
			case MQConstants.MQRC_CONFIG_REFRESH_OBJECT:
				changeType = "refresh";
				break;
				
			default:
				changeType = "unknwon";
				break;
		}
		return changeType;
		
	}
	
	/*
	 * Attribute Name
	 */
	private String AttributeName(PCFParameter bpcfParams) {

		String attribName = "";
		if (bpcfParams.getType() == MQConstants.MQCFT_BYTE_STRING) {
			attribName = MQConstants.lookup(bpcfParams.getParameter(), "MQBACF_.*").trim();
		}
		if (bpcfParams.getType() == MQConstants.MQCFT_STRING) {
			try {
				attribName = MQConstants.lookup(bpcfParams.getParameter(), "MQCACF_.*").trim();
			
			} catch (Exception e) {
				try {
					attribName = MQConstants.lookup(bpcfParams.getParameter(), "MQCACH_.*").trim();
					
				} catch (Exception e1) {
					attribName = MQConstants.lookup(bpcfParams.getParameter(), "MQCA_.*").trim();
				}
			}
			
		}
		if (bpcfParams.getType() == MQConstants.MQCFT_INTEGER) {
			try {
				attribName = MQConstants.lookup(bpcfParams.getParameter(), "MQIACF_.*").trim();

			} catch (Exception e) {
				try {
					attribName = MQConstants.lookup(bpcfParams.getParameter(), "MQIACH_.*").trim();
					
				} catch (Exception e2) {
					attribName = MQConstants.lookup(bpcfParams.getParameter(), "MQIA_.*").trim();
			//		attribName = "unknown";
				}	
			}
		}
		if (bpcfParams.getType() == MQConstants.MQCFT_INTEGER_LIST) {
			try {
				attribName = MQConstants.lookup(bpcfParams.getParameter(), "MQIACH_.*").trim();

			} catch (Exception e) {
				attribName = "unknown";				
			}
		}

		if (bpcfParams.getType() == MQConstants.MQCFT_INTEGER64) {
			attribName = "unknown";
		}
		
		if (attribName.equals("")) {
			attribName = "update_required";
		}
		
		return attribName;
		
	}
	
	@Override
	@PreDestroy
	public void Destroy()  {
		log.info("Destroy Config Events");		
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
