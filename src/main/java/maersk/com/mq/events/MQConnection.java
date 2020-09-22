package maersk.com.mq.events;

/*
 * Copyright 2020
 * Maersk
 *
 * Queue manager connection object
 * 
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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import com.ibm.mq.headers.pcf.PCFException;
import maersk.com.mq.channelevents.ChannelEvents;
import maersk.com.mq.configevents.ConfigEvents;
import maersk.com.mq.cpuevents.CPUEvents;
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
	
	@Value("${ibm.mq.queueManager}")
	private String queueManager;
	private String getQueueManagerName() {
		return this.queueManager;
	}
			
	@Value("${ibm.mq.keepMetricsWhenQueueManagerIsDown:false}")
	private boolean keepMetricsWhenQueueManagerIsDown;
	
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

    @Value("${ibm.mq.event.queuemanager.queue:#{null}}")
    private String queuemanagerqueuename;
    public String QueueManagerQueueName() {
    	return this.queuemanagerqueuename;
    }
    @Value("${ibm.mq.event.channel.queue:#{null}}")
    private String channelqueuename;
    public String ChannelQueueName() {
    	return this.channelqueuename;
    }
    @Value("${ibm.mq.event.config.queue:#{null}}")
    private String configqueuename;
    public String ConfigQueueName() {
    	return this.configqueuename;
    }
    @Value("${ibm.mq.event.cpu.queue:#{null}}")
    private String cpuqueuename;
    public String CPUQueueName() {
    	return this.cpuqueuename;
    }

    //@Autowired
    //private int connections = 3;
    //private Map<Integer,MQQueueManager>queManagers = new HashMap<Integer,MQQueueManager>();
    //private MQQueueManager queManager = null;
    //private MQQueueManager getQueueManager() {
    //	return this.queManager;
    //}
    //private void setQueueManager(MQQueueManager v) {
    //	this.queManager = v;
    //}
    
   // private PCFMessageAgent messageagent = null;
   // private PCFMessageAgent getMessageAgent() {
   // 	return this.messageagent;
   // }
   // private void setMessageAgent(PCFMessageAgent v) {
   // 	this.messageagent = v;
   // }
    
	//@Autowired
	//private MQMonitorBase base;
	
	@Autowired
	private MeterRegistry meterRegistry;
	
	@Autowired
    private ChannelEvents channelevents;
    public ChannelEvents ChannelEventsObject() {
    	return this.channelevents;
    }

    @Autowired
    private QueueMangerEvents queuemanagerevents;
    public QueueMangerEvents QueueManagerEventsObject() {
    	return this.queuemanagerevents;
    }

    @Autowired
    private ConfigEvents configevents;
    public ConfigEvents ConfigEventsObject() {
    	return this.configevents;
    }

    @Autowired
    private CPUEvents cpuevents;
    public CPUEvents CPUEventsObject() {
    	return this.cpuevents;
    }
    
    @Autowired
    private MQMetricsQueueManager mqmetricsqueuemanager;
    public MQMetricsQueueManager MetricQueueManager() {
    	return this.mqmetricsqueuemanager;
    }

    
	@Autowired
	private ThreadPoolTaskExecutor executors;
	private ThreadPoolTaskExecutor Executors() {
		return this.executors;
	}
	//private void Executors(ThreadPoolTaskExecutor v) {
	//	this.executors = v;
	//}
    
	//@Autowired
	//private ExecutorService es;
	//private ExecutorService getES() {
	//	return this.es;
	//}
	//private void setES(ExecutorService v) {
	//	this.es = v;
	//}

	/*
	 * Futures ...
	 */
	private Future<Integer> futureQueueManager = null;
	private void FutureQueueManager(Future<Integer> v) {
		this.futureQueueManager = v;
	}
	private Future<Integer> FutureQueueManager() {
		return this.futureQueueManager;
	}
	
	private Future<Integer> futureChannel = null;
	private void FutureChannel(Future<Integer> v) {
		this.futureChannel = v;
	}
	private Future<Integer> FutureChannel() {
		return this.futureChannel;
	}

	private Future<Integer> futureConfig = null;
	private void FutureConfig(Future<Integer> v) {
		this.futureConfig = v;
	}
	private Future<Integer> FutureConfig() {
		return this.futureConfig;
	}

	private Future<Integer> futureCPU = null;
	private void FutureCPU(Future<Integer> v) {
		this.futureCPU = v;
	}
	private Future<Integer> FutureCPU() {
		return this.futureCPU;
	}
		
    @Bean
    public JSONController JSONController() {
    	return new JSONController();
    }
        
    private Map<String,AtomicInteger>queueManagerStatusMap = new HashMap<String,AtomicInteger>();
    protected static final String queueManagerStatus = "mq:connectedToQueueManager";

    
    // Constructor
	public MQConnection() {
	}	
	
	@Bean("threadPoolTaskExecutor")
	public final ThreadPoolTaskExecutor AsyncExecutor() {
		
		int count = EventsRequired();
		
		log.debug("Creating executors");
		ThreadPoolTaskExecutor ex = new ThreadPoolTaskExecutor();
		ex.setCorePoolSize(count);
		ex.setMaxPoolSize(count);
		ex.setWaitForTasksToCompleteOnShutdown(true);
		ex.setThreadNamePrefix("mq-events");
		ex.initialize();
		return ex;
	}

	/*
	 * Count the number of queues to determine the number of types of events required
	 */
	private int EventsRequired() {
		
		int count = 0;
		if (QueueManagerQueueName() != null) {
			count++;
		}
		if (ChannelQueueName() != null) {
			count++;
		}
		if (ConfigQueueName() != null) {
			count++;
		}
		if (CPUQueueName() != null) {
			count++;
		}
		return count;
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
		
		//Executors(AsyncExecutor());
		
		/*
		 * Make a connection to the queue manager and start the threads
  		 */
		CreateQueueManagerConnectionAndStartThreads();		
				
	}
	
	/**
	 * Create an ExecutorService object
	 * Connect to a queue manager
	 * If we have a queue manager object, start each thread 
	 * 
	 */
	private void CreateQueueManagerConnectionAndStartThreads()  {

		//Executors(AsyncExecutor());

		/*
		 * Queue manager events
		 */
		//}
		if (QueueManagerQueueName() != null) {
			if (QueueManagerEventsObject() != null) {
				if (QueueManagerEventsObject().TaskStatus() == IMQPCFConstants.TASK_STOPPED) {
					FutureQueueManager(Executors().submit(QueueManagerEventsObject()));
				}
			}
		}
		
		/*
		 * Channel events
		 */
		if (ChannelQueueName() != null) { 
			if (ChannelEventsObject() != null) {
				if (ChannelEventsObject().TaskStatus() == IMQPCFConstants.TASK_STOPPED) {
					FutureChannel(Executors().submit(ChannelEventsObject()));
				}
			}
		}
		
		/*
		 * Config events
		 */
		if (ConfigQueueName() != null) {
			if (ConfigEventsObject() != null) {
				if (ConfigEventsObject().TaskStatus() == IMQPCFConstants.TASK_STOPPED) {
					FutureConfig(Executors().submit(ConfigEventsObject()));
				}
			}
		}
		
		/*
		 * CPU events
		 */
		if (CPUQueueName() != null) {
			if (CPUEventsObject() != null) {
				if (CPUEventsObject().TaskStatus() == IMQPCFConstants.TASK_STOPPED) {
					FutureCPU(Executors().submit(CPUEventsObject()));
				}
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
		if (FutureQueueManager() != null) {
			s = FutureQueueManager().isDone();
			log.debug("Start Stop service future : {}", s);
		}
		
		boolean c = false;
		if (FutureChannel() != null) {
			c = FutureChannel().isDone();
			log.debug("Channel service future : {}", c);
		}
		
		boolean c1 = false;
		if (FutureConfig() != null) {
			c1 = FutureConfig().isDone();
			log.debug("Config service future : {}", c);
		}

		boolean c2 = false;
		if (FutureCPU() != null) {
			c2 = FutureCPU().isDone();
			log.debug("CPU service future : {}", c);
		}
		
		/*
		 * If any thread has stopped, disconnect and restart the threads
		 */
		if ((s) || (c) || (c1) || (c2)) {
			Disconnect();
			CreateQueueManagerConnectionAndStartThreads();
		
		} else {
			QueueManagerStatus(IMQPCFConstants.CONNECTED_TO_QM);

		}
		
	}
	
	/*
	 * Set Queue Manager status
	 */
	private void QueueManagerStatus(int val) {

		final String label = queueManagerStatus + "_" + getQueueManagerName();

		AtomicInteger value = queueManagerStatusMap.get(label);
		if (value == null) {
			queueManagerStatusMap.put(label,
					meterRegistry.gauge(queueManagerStatus, 
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
    public void Disconnect() {
    	FutureChannel().cancel(true);
    	FutureQueueManager().cancel(true);
    	FutureConfig().cancel(true);
    	FutureCPU().cancel(true);
    	
    	Executors().shutdown();    	
    	CloseQMConnection();
    }
    
	        
    public void CloseQMConnection() {

		log.info("Disconnected from the queue manager");
		QueueManagerStatus(IMQPCFConstants.NOT_CONNECTED_TO_QM);

    }
    
}


