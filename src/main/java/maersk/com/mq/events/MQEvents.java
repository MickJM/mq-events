package maersk.com.mq.events;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@ComponentScan(basePackages = { "maersk.com.mq.events"} )
@ComponentScan("maersk.com.mq.channelevents")
@ComponentScan("maersk.com.mq.queuemanagerevents")
@ComponentScan("maersk.com.mq.configevents")
@ComponentScan("maersk.com.mq.cpuevents")
@SpringBootApplication
@EnableScheduling
public class MQEvents {

	public static void main(String[] args) {
		SpringApplication sa = new SpringApplication(MQEvents.class);
		sa.run(args);
		
	}
}
