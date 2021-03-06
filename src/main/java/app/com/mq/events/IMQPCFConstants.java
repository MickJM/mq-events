package app.com.mq.events;

public interface IMQPCFConstants {

		public int BASE = 0;
		public int PCF_INIT_VALUE = 0;
		public int NOTSET = -1;
		public int MULTIINSTANCE = 1;
		public int NOT_MULTIINSTANCE = 0;
		public int MODE_LOCAL = 0;
		public int MODE_CLIENT = 1;
		public int EXIT_ERROR = 1;
		
		public int NONE = 0;
		public int INFO = 1;
		public int DEBUG = 2;
		public int WARN = 4;
		public int ERROR = 8;
		public int TRACE = 16;
		
		public int NOT_CONNECTED_TO_QM = 0;
		public int CONNECTED_TO_QM = 1;
		
		/*
		 * For running tasks
		 */
		public int TASK_STARTING = 1;
		public int TASK_RUNNING = 2;
		public int TASK_STOPPING = 3;
		public int TASK_STOPPED = 4;

		public int SEQ_FIRST = 1;
		public int SEQ_SECOND = 2;

		public String QE_BEFORE = "BEFORE";
		public String QE_AFTER = "AFTER";

		public int OKAY = 0;
		public int RET_WITH_ERROR = 20;
}
