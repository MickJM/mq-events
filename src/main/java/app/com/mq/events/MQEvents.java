package app.com.mq.events;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@ComponentScan(basePackages = { "app.com.mq.events"} )
@ComponentScan("app.com.mq.channelevents")
@ComponentScan("app.com.mq.queuemanagerevents")
@ComponentScan("app.com.mq.configevents")
@ComponentScan("app.com.mq.cpuevents")
@SpringBootApplication
@EnableScheduling
public class MQEvents {

	public static void main(String[] args) {
		SpringApplication sa = new SpringApplication(MQEvents.class);
		sa.run(args);
		
	}
}
