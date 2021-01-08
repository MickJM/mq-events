package app.com.mq.events;

/*
 * Copyright 2020
 * Maersk
 *
 * https://community.ibm.com/community/user/imwuc/viewdocument/a-first-look-at-mq-resource-usage-s?CommunityKey=183ec850-4947-49c8-9a2e-8e7c7fc46c64&tab=librarydocuments
 *
 * Queue Manager obkect
 *  
 */

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.pcf.MQCFGR;
import com.ibm.mq.headers.pcf.MQCFH;
import com.ibm.mq.headers.pcf.MQCFIL;
import com.ibm.mq.headers.pcf.MQCFST;
import com.ibm.mq.headers.pcf.PCFMessage;
import com.ibm.mq.headers.pcf.PCFMessageAgent;
import com.ibm.mq.headers.pcf.PCFParameter;

import app.com.mq.metrics.accounting.AccountingEntity;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

@Component
public class MQMetricsQueueManager {

	private final static Logger log = LoggerFactory.getLogger(MQMetricsQueueManager.class);
				
	private boolean onceonly = true;
	public void OnceOnly(boolean v) {
		this.onceonly = v;
	}
	public boolean OnceOnly() {
		return this.onceonly;
	}
	
	// taken from connName
	private String hostname;
	public void HostName(String v) {
		this.hostname = v;
	}
	public String HostName() { return this.hostname; }
	
	@Value("${ibm.mq.queueManager}")
	private String queuemanagername;
	public void QueueManagerName(String v) {
		this.queuemanagername = v;
	}
	public String QueueManagerName() { return this.queuemanagername; }
	
	// hostname(port)
	@Value("${ibm.mq.connName}")
	private String connectionname;
	public void ConnectionName(String v) {
		this.connectionname = v;
	}
	public String ConnectionName() { return this.connectionname; }
	
	@Value("${ibm.mq.channel}")
	private String channel;
	public void ChannelName(String v) {
		this.channel = v;
	}
	public String ChannelName() { return this.channel; }

	// taken from connName
	private int port;
	public void Port(int v) {
		this.port = v;
	}
	public int Port() { return this.port; }

	@Value("${ibm.mq.user:#{null}}")
	private String userid;
	public void UserId(String v) {
		this.userid = v;
	}
	public String UserId() { return this.userid; }

	@Value("${ibm.mq.password:#{null}}")
	private String password;
	public void Password(String v) {
		this.password = v;
	}
	public String Password() { return this.password; }

	// MQ Connection Security Parameter
	@Value("${ibm.mq.authenticateUsingCSP:true}")
	private boolean authcsp;
	public boolean getMQCSP() {
		return this.authcsp;
	}
	
	@Value("${ibm.mq.useSSL:false}")
	private boolean usessl;
	public boolean UsingSSL() {
		return this.usessl;
	}
	
	@Value("${ibm.mq.security.truststore:}")
	private String truststore;
	public String TrustStore() {
		return this.truststore;
	}
	@Value("${ibm.mq.security.truststore-password:}")
	private String truststorepass;
	public String TrustStorePass() {
		return this.truststorepass;
	}
	@Value("${ibm.mq.security.keystore:}")
	private String keystore;
	public String KeyStore() {
		return this.keystore;
	}
	@Value("${ibm.mq.security.keystore-password:}")
	private String keystorepass;
	public String KeyStorePass() {
		return this.keystorepass;
	}
	
	@Value("${ibm.mq.sslCipherSpec}")
	private String cipher;
	public String Cipher() {
		return this.cipher;
	}

	@Value("${ibm.mq.security.ibmcipher:false}")
	private String ibmcipher;
	public String IBMCipher() {
		return this.ibmcipher;
	}

	@Value("${ibm.mq.multiInstance:false}")
	private boolean multiinstance;
	public boolean MultiInstance() {
		return this.multiinstance;
	}
	
	@Value("${ibm.mq.ccdtFile:#{null}}")
	private String ccsdfile;
	public String CCDTFile() {
		return this.ccsdfile;
	}	
	
	@Value("${ibm.mq.local:false}")
	private boolean local;
	public boolean RunningLocal() {
		return this.local;
	}
			
	@Value("${ibm.mq.pcf.browse:false}")
	private boolean browse;	
	public boolean Browse() {
		return this.browse;
	}
	public void Browse(boolean v) {
		this.browse = v;
	}
	
	@Value("${info.app.version:}")
	private String appversion;	
	public String getVersionNumeric() {
		return this.appversion;
	}
	
	@Value("${info.app.name:MQMonitor}")
	private String appName;	
	public String getAppName() {
		return this.appName;
	}
	
	//private int stattype;
	//private void StateType(int v) {
	//	this.stattype = v;
	//}
	//private int StatType() {
	//	return this.stattype;
	//}
	
	/*
     *  MAP details for the metrics
     */
    private Map<String,AtomicInteger>runModeMap = new HashMap<String,AtomicInteger>();
    private Map<String,AtomicInteger>versionMap = new HashMap<String,AtomicInteger>();

    protected static final String runMode = "mq:runMode";
    protected static final String version = "mq:monitoringVersion";
	
	/*
	 * Validate connection name and userID
	 */
	private boolean ValidConnectionName() {
		return (ConnectionName().equals(""));
	}
	private boolean ValidateUserId() {
		return (UserId().equals(""));		
	}
	private boolean ValidateUserId(String v) {
		boolean ret = false;
		if (UserId().equals(v)) {
			ret = true;
		}
		return ret;
	}
	
    private MQQueueManager queManager;
    public void QueueManager(MQQueueManager qm) {
    	this.queManager = qm;
    }
    public MQQueueManager QueueManager() {
    	return this.queManager;
    }

    private MQQueue queue = null;
    public void Queue(MQQueue q) {
    	this.queue = q;
    }
    public MQQueue Queue() {
    	return this.queue;
    }
    
    private MQGetMessageOptions gmo = null;
    public void GetMessageOptions(MQGetMessageOptions gmo) {
    	this.gmo = gmo;
    }
    public MQGetMessageOptions GetMessageOptions() {
    	return this.gmo;
    }
    
	private int qmgrAccounting;
	public synchronized void Accounting(int v) {		
		this.qmgrAccounting = v;
	}
	public synchronized int Accounting() {		
		return this.qmgrAccounting;
	}
	private int stats;
	public synchronized void QueueManagerStatistics(int v) {
		this.stats = v;
	}
	public synchronized int QueueManagerStatistics() {
		return this.stats;
	}

	@Autowired
	private MeterRegistry meterRegistry;

    @Autowired
    Environment env;

    /*
     * Constructor
     */
	public MQMetricsQueueManager() {
	}
	
	@PostConstruct
	public void Init() {
		RunMode();
		Version();
		
	}
		
	/*
	 * Create an MQQueueManager object
	 */
	@SuppressWarnings("rawtypes")
	public MQQueueManager CreateQueueManager() throws MQException, MQDataException, MalformedURLException {

		Hashtable<String, Comparable> env = new Hashtable<String, Comparable>();
		
		if (!RunningLocal()) { 
			
			SetEnvironmentVariables();
			log.info("Attempting to connect using a client connection");

			if ((CCDTFile() == null) || (CCDTFile().isEmpty())) {
				env.put(MQConstants.HOST_NAME_PROPERTY, HostName());
				env.put(MQConstants.CHANNEL_PROPERTY, ChannelName());
				env.put(MQConstants.PORT_PROPERTY, Port());
			}
			
			/*
			 * 
			 * If a username and password is provided, then use it
			 * ... if CHCKCLNT is set to OPTIONAL or RECDADM
			 * ... RECDADM will use the username and password if provided ... if a password is not provided
			 * ...... then the connection is used like OPTIONAL
			 */		
		
			if (!StringUtils.isEmpty(UserId())) {
				env.put(MQConstants.USER_ID_PROPERTY, UserId()); 
				if (!StringUtils.isEmpty(Password())) {
					env.put(MQConstants.PASSWORD_PROPERTY, Password());
				}
			}
			env.put(MQConstants.USE_MQCSP_AUTHENTICATION_PROPERTY, getMQCSP());
			env.put(MQConstants.TRANSPORT_PROPERTY,MQConstants.TRANSPORT_MQSERIES);
			env.put(MQConstants.APPNAME_PROPERTY,getAppName());
			
			if (MultiInstance()) {
				if (OnceOnly()) {
					OnceOnly(false);
					log.info("MQ Metrics is running in multiInstance mode");
				}
			}
			
			log.debug("Host     : {} ", HostName());
			log.debug("Channel  : {} ", ChannelName());
			log.debug("Port     : {} ", Port());
			log.debug("Queue Man: {} ", QueueManagerName());
			log.debug("User     : {} ", UserId());
			log.debug("Password : {} ", "*******");
			if (UsingSSL()) {
				log.debug("SSL is enabled ....");
			}
			
			// If SSL is enabled (default)
			if (UsingSSL()) {
				if (!StringUtils.isEmpty(TrustStore())) {
					System.setProperty("javax.net.ssl.trustStore", TrustStore());
			        System.setProperty("javax.net.ssl.trustStorePassword", TrustStorePass());
			        System.setProperty("javax.net.ssl.trustStoreType","JKS");
			        System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings",IBMCipher());
				}
				if (!StringUtils.isEmpty(KeyStore())) {
			        System.setProperty("javax.net.ssl.keyStore", KeyStore());
			        System.setProperty("javax.net.ssl.keyStorePassword", KeyStorePass());
			        System.setProperty("javax.net.ssl.keyStoreType","JKS");
				}
				if (!StringUtils.isEmpty(Cipher())) {
					env.put(MQConstants.SSL_CIPHER_SUITE_PROPERTY, Cipher());
				}
			
			} else {
				log.debug("SSL is NOT enabled ....");
			}
			
			if (!StringUtils.isEmpty(TrustStore())) {
				log.debug("TrustStore       : {} " ,TrustStore());
				log.debug("TrustStore Pass  : {} ", "********");
			}
			if (!StringUtils.isEmpty(KeyStore())) {
				log.debug("KeyStore         : {} " ,KeyStore());
				log.debug("KeyStore Pass    : {} " , "********");
				log.debug("Cipher Suite     : {} " ,Cipher());
			}
		} else {
			log.debug("Attemping to connect using local bindings");
		}
				
		/*
		 * Connect to the queue manager 
		 * ... local connection : application connection in local bindings
		 * ... client connection: application connection in client mode 
		 */
		MQQueueManager qmgr = null;
		if (RunningLocal()) {
			log.info("Attemping to connect to queue manager {} using local bindings", QueueManagerName());
			qmgr = new MQQueueManager(QueueManagerName());
			
		} else {
			if ((CCDTFile() == null) || (CCDTFile().isEmpty())) {
				log.info("Attempting to connect to queue manager {} client connection" ,QueueManagerName());
				qmgr = new MQQueueManager(QueueManagerName(), env);
				
			} else {
				URL ccdtFileName = new URL("file:///" + CCDTFile());
				log.info("Attempting to connect to queue manager {} using CCDT file", QueueManagerName());
				qmgr = new MQQueueManager(this.queuemanagername, env, ccdtFileName);
				
			}
		}
		log.info("Connection to queue manager established ");		
		return qmgr;
	}
	
	/*
	 * Create a PCF agent
	 */	
	public PCFMessageAgent createMessageAgent(MQQueueManager queManager) throws MQDataException {
		
		log.info("Attempting to create a PCFAgent ");
		PCFMessageAgent pcfmsgagent = new PCFMessageAgent(queManager);
		log.info("PCFAgent created successfully");
	
		return pcfmsgagent;	
		
	}
	
	/*
	 * Get MQ details from environment variables
	 */
	private void SetEnvironmentVariables() {
		
		/*
		 * ALL parameter are passed in the application.yaml file ...
		 *    These values can be overrided using an application-???.yaml file per environment
		 *    ... or passed in on the command line
		 */
		
		// Split the host and port number from the connName ... host(port)
		if (!ValidConnectionName()) {
			Pattern pattern = Pattern.compile("^([^()]*)\\(([^()]*)\\)(.*)$");
			Matcher matcher = pattern.matcher(this.connectionname);	
			if (matcher.matches()) {
				this.hostname = matcher.group(1).trim();
				this.port = Integer.parseInt(matcher.group(2).trim());
			
			} else {
				log.error("While attempting to connect to a queue manager, the connName is invalid ");
				System.exit(IMQPCFConstants.EXIT_ERROR);				
			
			}
			
		} else {
			log.error("While attempting to connect to a queue manager, the connName is missing  ");
			System.exit(IMQPCFConstants.EXIT_ERROR);
			
		}

		/*
		 * If we dont have a user or a certs are not being used, then we cant connect ... unless we are in local bindings
		 */
		if (ValidateUserId()) {
			if (!UsingSSL()) {
				log.error("Unable to connect to queue manager, credentials are missing and certificates are not being used");
				System.exit(IMQPCFConstants.EXIT_ERROR);
			}
		}

		// if no user, forget it ...
		if (UserId() == null) {
			return;
		}

		/*
		 * dont allow mqm user
		 */
		if (!ValidateUserId()) {
			if ((ValidateUserId("mqm") || (ValidateUserId("MQM")))) {
				log.error("The MQ channel USERID must not be running as 'mqm' ");
				System.exit(IMQPCFConstants.EXIT_ERROR);
			}
		} else {
			UserId(null);
			Password(null);
		}
	}
	
	/*
	 * Set 'runmode'
	 *    LOCAL or CLIENT
	 */
	private void RunMode() {

		int mode = IMQPCFConstants.MODE_LOCAL;
		if (!RunningLocal()) {
			mode = IMQPCFConstants.MODE_CLIENT;
		}
		
		runModeMap.put(runMode, meterRegistry.gauge(runMode, 
				Tags.of("queueManagerName", QueueManagerName()),
				new AtomicInteger(mode)));
	}

	/*
	 * Parse the version
	 */
	private void Version() {

		String s = getVersionNumeric().replaceAll("[\\s.]", "");
		int v = Integer.parseInt(s);		
		versionMap.put(version, meterRegistry.gauge(version, 
				new AtomicInteger(v)));		
	}

	/*
	 * Close the connection to the queue
	 */
	public void CloseQueue(MQQueue queue) {
		
    	try {
    		log.debug("Closing Queue Connection ");
			queue.close();
			
    	} catch (Exception e) {
    		// do nothing
    	}
    	
	}
	
	
	/*
	 * Close the connection to the queue manager
	 */
	public void CloseConnection(MQQueueManager qm) {
		
    	try {
    		if (qm.isConnected()) {
	    		log.debug("Closing MQ Connection ");
    			qm.disconnect();
    		}
    	} catch (Exception e) {
    		// do nothing
    	}
    	
	}

	
}
