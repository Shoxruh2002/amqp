package uz.jl.subscriber;

import lombok.*;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.sql.Timestamp;


@EnableRabbit
@SpringBootApplication
public class SubscriberApplication {

    public static void main( String[] args ) {
        SpringApplication.run(SubscriberApplication.class, args);
    }

}

@Configuration
class RabbitMQConfig {
    public static final String QUEUE = "xt_in";
    public static final String EXCHANGE = "pdp_exchange";
    public static final String ROUTING_KEY = "pdp_routing_key";

    @Bean
    public MessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }


    @Primary
    @Bean("connection-factory-prod")
    public ConnectionFactory connectionFactoryProd() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5682);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        return connectionFactory;
    }

    @Bean(name = "connection-factory-dev")
    public ConnectionFactory connectionFactoryDev() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        return connectionFactory;
    }

    @Bean(name = "rabbit-listener-container-factory-dev")
    public SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactoryDev(
            SimpleRabbitListenerContainerFactoryConfigurer configurer,
            @Qualifier("connection-factory-dev") ConnectionFactory connectionFactory ) {

        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        configurer.configure(factory, connectionFactory);

        factory.setDefaultRequeueRejected(false);

        return factory;
    }

    @Bean(name = "rabbit-listener-container-factory-prod")
    public SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactoryProd(
            SimpleRabbitListenerContainerFactoryConfigurer configurer,
            @Qualifier("connection-factory-prod") ConnectionFactory connectionFactory ) {

        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        configurer.configure(factory, connectionFactory);

        factory.setDefaultRequeueRejected(false);

        return factory;
    }


}

@Component
class TransactionListener {

    @RabbitListener(containerFactory = "rabbit-listener-container-factory-dev", queues = RabbitMQConfig.QUEUE)
    public void logTransactiondev( Transaction transaction ) {
        System.out.println("from dev" + transaction);
    }
    @RabbitListener(containerFactory = "rabbit-listener-container-factory-prod", queues = RabbitMQConfig.QUEUE)
    public void logTransactionprod( Transaction transaction ) {
        System.out.println("from prod" + transaction);
    }
//    @RabbitListener( queues = RabbitMQConfig.QUEUE)
//    public void logTransactionprod( Transaction transaction ) {
//        System.out.println("from prod" + transaction);
//    }
}


@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
class Transaction {
    private Long id;
    private String pan;
    private BigDecimal amount;
    private Timestamp createdAt;

}
