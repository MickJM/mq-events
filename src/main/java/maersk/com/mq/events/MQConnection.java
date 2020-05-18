package maersk.com.mq.events;

/*
 * Copyright 2019
 * Maersk
 *
 * Connect to a queue manager
 * 
 * 22/10/2019 - Capture the return code when the queue manager throws an error so multi-instance queue
 *              managers can be checked
 */

import java.io.IOException;
import java.lang.ProcessHandle.Info;
import java.net.MalformedURLException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.MQExceptionWrapper;
import com.ibm.mq.headers.pcf.PCFMessageAgent;

import io.micrometer.core.instrument.Tags;

import com.ibm.mq.headers.pcf.PCFException;

import maersk.com.mq.channelevents.ChannelEvents;
import maersk.com.mq.configevents.ConfigEvents;
import maersk.com.mq.json.controller.JSONController;
import maersk.com.mq.queuemanagerevents.QueueMangerEvents;

@Component
public class MQConnection {

    private final static Logger log = LoggerFactory.getLogger(MQConnection.class);

	@Value("${application.debug:false}")
    protected boolean _debug;
	
	@Value("${application.debugLevel:NONE}")
	protected String _debugLevel;
    
	@Value("${application.save.metrics.required:false}")
    private boolean summaryRequired;

	@Value("${ibm.mq.multiInstance:false}")
	private boolean multiInstance;
	public boolean isMultiInstance() {
		return this.multiInstance;
	}
	
	@Value("${ibm.mq.queueManager}")
	private String queueManager;
	private String getQueueManagerName() {
		return this.queueManager;
	}
		
	@Value("${ibm.mq.local:false}")
	private boolean local;
	public boolean isRunningLocal() {
		return this.local;
	}
	
	@Value("${ibm.mq.keepMetricsWhenQueueManagerIsDown:false}")
	private boolean keepMetricsWhenQueueManagerIsDown;
	
	//
	@Value("${ibm.mq.useSSL:false}")
	private boolean bUseSSL;
	public boolean usingSSL() {
		return this.bUseSSL;
	}
	
	@Value("${ibm.mq.security.truststore:}")
	private String truststore;
	@Value("${ibm.mq.security.truststore-password:}")
	private String truststorepass;
	@Value("${ibm.mq.security.keystore:}")
	private String keystore;
	@Value("${ibm.mq.security.keystore-password:}")
	private String keystorepass;
	
    @Value("${ibm.mq.event.delayInMilliSeconds:10000}")
    private long resetIterations;

    //@Autowired
    private int connections = 3;
    private Map<Integer,MQQueueManager>queManagers = new HashMap<Integer,MQQueueManager>();
    private MQQueueManager queManager = null;
    private MQQueueManager getQueueManager() {
    	return this.queManager;
    }
    private void setQueueManager(MQQueueManager v) {
    	this.queManager = v;
    }
    
    
    
    private PCFMessageAgent messageAgent = null;
    private PCFMessageAgent getMessageAgent() {
    	return this.messageAgent;
    }
    private void setMessageAgent(PCFMessageAgent v) {
    	this.messageAgent = v;
    }
    
	@Autowired
	private MQMonitorBase base;
	
	@Autowired
    private ChannelEvents channelEvents;
    private ChannelEvents getChannelEventsObject() {
    	return this.channelEvents;
    }

    @Autowired
    private QueueMangerEvents queueMangerEvents;
    private QueueMangerEvents getQueueManagerEventsObject() {
    	return this.queueMangerEvents;
    }

    @Autowired
    private ConfigEvents configEvents;
    private ConfigEvents getConfigEventsObject() {
    	return this.configEvents;
    }

    @Autowired
    public MQMetricsQueueManager mqMetricsQueueManager;
    private MQMetricsQueueManager getMetricQueueManager() {
    	return this.mqMetricsQueueManager;
    }
    
	//@Autowired
	private ThreadPoolTaskExecutor executors;
	private ThreadPoolTaskExecutor getExecutors() {
		return this.executors;
	}
	private void setExecutors(ThreadPoolTaskExecutor v) {
		this.executors = v;
	}
    
	//@Autowired
	private ExecutorService es;
	private ExecutorService getES() {
		return this.es;
	}
	private void setES(ExecutorService v) {
		this.es = v;
	}

	/*
	 * Futures ...
	 */
	private Future<Integer> futureQueueManager = null;
	private void setFutureQueueManager(Future<Integer> v) {
		this.futureQueueManager = v;
	}
	private Future<Integer> getFutureQueueManager() {
		return this.futureQueueManager;
	}
	
	private Future<Integer> futureChannel = null;
	private void setFutureChannel(Future<Integer> v) {
		this.futureChannel = v;
	}
	private Future<Integer> getFutureChannel() {
		return this.futureChannel;
	}

	private Future<Integer> futureConfig = null;
	private void setFutureConfig(Future<Integer> v) {
		this.futureConfig = v;
	}
	private Future<Integer> getFutureConfig() {
		return this.futureConfig;
	}
	
	/*
	 * ************ 
	 */
	
	
    @Bean
    public JSONController JSONController() {
    	return new JSONController();
    }
        
    private Map<String,AtomicInteger>queueManagerStatusMap = new HashMap<String,AtomicInteger>();
    protected static final String queueManagerStatus = "mq:connectedToQueueManager";

	private MQQueueManager qmadmin = null;
	private MQQueueManager qmchannel = null;
	private MQQueueManager qmconfig = null;
	
    
    // Constructor
	public MQConnection() {
	}	
	
	//@Bean("threadPoolTaskExecutor")
	public final ThreadPoolTaskExecutor getAsyncExecutor() {
		
		log.info("Creating executors");
		ThreadPoolTaskExecutor ex = new ThreadPoolTaskExecutor();
		ex.setCorePoolSize(3);
		ex.setMaxPoolSize(4);
		ex.setWaitForTasksToCompleteOnShutdown(true);
		ex.setThreadNamePrefix("mq-events");
		ex.initialize();
		return ex;
	}

	/**
	 * Connect to a queue manager and start all threads required 
	 * 
	 * @throws MQException
	 * @throws MQDataException
	 * @throws MalformedURLException
	 */
	@PostConstruct
	public void startThreads() throws MQException, MQDataException, MalformedURLException {
		
		/*
		 * Make a connection to the queue manager and start the tasks
  		 */
		createQueueManagerConnectionAndStartThreads();		
				
	}
	
	/**
	 * Create an ExecutorService object
	 * Connect to a queue manager
	 * If we have a queue manager object, start each thread 
	 * 
	 */
	
	private void createQueueManagerConnectionAndStartThreads()  {

		setExecutors(getAsyncExecutor());
		//connectToQueueManager();
				
		/*
		 * Start each thread
		 */
		//}
		if (getQueueManagerEventsObject() != null) {
			if (getQueueManagerEventsObject().taskStatus() == MQPCFConstants.TASK_STOPPED) {
		//		getQueueManagerEventsObject().setQueueManager(qmadmin);
				setFutureQueueManager(getExecutors().submit(getQueueManagerEventsObject()));
			}
		}

		/*
		 * Channel events
		 */		
		if (getChannelEventsObject() != null) {
			if (getChannelEventsObject().taskStatus() == MQPCFConstants.TASK_STOPPED) {
			//	getChannelEventsObject().setQueueManager(qmchannel);
				setFutureChannel(getExecutors().submit(getChannelEventsObject()));
			}
		}
			
		
		/*
		 * Config events
		 */
		if (getConfigEventsObject() != null) {
			if (getConfigEventsObject().taskStatus() == MQPCFConstants.TASK_STOPPED) {
			//	getConfigEventsObject().setQueueManager(qmconfig);
				setFutureConfig(getExecutors().submit(getConfigEventsObject()));
			}
		}
		
	}
	
	
	/**
	 * Every 'x' seconds, check the status of the running tasks
	 * 
	 * Each task will run for as long as the API is running ...
	 * 
	 * If either task fails, then stop ALL tasks and reconnect and continue
	 * 
	 */
	@Scheduled(fixedDelayString="${ibm.mq.event.delayInMilliSeconds}")
	public void checkStatus() throws MalformedURLException, MQException, MQDataException {
		
		boolean s = false;
		if (getFutureQueueManager() != null) {
			s = getFutureQueueManager().isDone();
			log.info("Start Stop service future : {}", s);
		}
		
		boolean c = false;
		if (getFutureChannel() != null) {
			c = getFutureChannel().isDone();
			log.info("Channel service future : {}", c);
		}
		
		boolean c1 = false;
		if (getFutureConfig() != null) {
			c1 = getFutureConfig().isDone();
			log.info("Config service future : {}", c);
		}
		
		if ((s) || (c) || (c1) ) {
			disconnect();
			createQueueManagerConnectionAndStartThreads();
		} else {
			connectedToQueueManager();
		}
		
	}
	
			
	private void connectedToQueueManager() {

		queueManagerStatus(MQPCFConstants.CONNECTED_TO_QM);
		
	}
	
	
	private void queueManagerStatus(int val) {

		final String label = queueManagerStatus + "_" + getQueueManagerName();

		AtomicInteger value = queueManagerStatusMap.get(label);
		if (value == null) {
			queueManagerStatusMap.put(label,
					base.meterRegistry.gauge(queueManagerStatus, 
					Tags.of("queueManagerName", getQueueManagerName()
							),
					new AtomicInteger(val))
					);
		} else {
			value.set(val);
		}		

	}

	
	/**
	 * Disconnect cleanly from the queue manager
	 */
    @PreDestroy
    public void disconnect() {
    	getFutureChannel().cancel(true);
    	getFutureQueueManager().cancel(true);
    
    	getExecutors().shutdown();    	
    	closeQMConnection();
    }
    
	        
    public void closeQMConnection() {

		log.info("Disconnected from the queue manager");
		//if (qmadmin != null) {
		//	getMetricQueueManager().CloseConnection(qmadmin);
		//	qmadmin = null;
		//}
		//if (qmchannel != null) {
		//	getMetricQueueManager().CloseConnection(qmchannel);
		//	qmchannel = null;
		//}
		//if (qmconfig != null) {
		//	getMetricQueueManager().CloseConnection(qmconfig);
		//	qmconfig = null;
		//}

		queueManagerStatus(MQPCFConstants.NOT_CONNECTED_TO_QM);

    }
    
}


