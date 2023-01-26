package uz.jl.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.Formula;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import java.math.BigDecimal;
import java.sql.Timestamp;

@SpringBootApplication
@EnableRabbit
public class PublisherApplication {

    public static void main( String[] args ) {
        SpringApplication.run(PublisherApplication.class, args);
    }

}


@Getter
@Setter
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
class Transaction {

    @Id
    @GeneratedValue
    private Long id;

    @Column(nullable = false)
    private String pan;

    @Column(nullable = false)
    @Formula("amount > 0.0")
    private BigDecimal amount;

    @CreatedDate
    @CreationTimestamp
    @Column(columnDefinition = "timestamp default current_timestamp")
    private Timestamp createdAt;

}


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
class TransferCreateVO {
    private String pan;
    private BigDecimal amount;
}

interface TransactionRepository extends JpaRepository<Transaction, Long> {
}

@Service
@RequiredArgsConstructor
class TransactionService {

    private final TransactionRepository repository;
    private final RabbitMQService rabbitMQService;

    public Transaction create( TransferCreateVO vo ) {
        Transaction transaction = new Transaction(null, vo.getPan(), vo.getAmount(), null);
        transaction = repository.save(transaction);
        try {
            rabbitMQService.send(transaction, "prod");
            rabbitMQService.send(transaction, "dev");
        } catch ( Exception e ) {
            e.printStackTrace();
        }

        return transaction;
    }
}


@RestController
@RequestMapping("/transfer")
@RequiredArgsConstructor
class PublisherController {

    private final TransactionService service;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Transaction transfer( @RequestBody TransferCreateVO vo ) {
        return service.create(vo);
    }
}

@Configuration
class RabbitMQConfig {
    public static final String QUEUE = "pdp_queue";
    public static final String EXCHANGE = "common";
    public static final String ROUTING_KEY = "pdp_routing_key";

    //    @Bean
//    public Queue queue() {
//        return new Queue(QUEUE, true);
//    }
//
//    @Bean
//    public TopicExchange exchange() {
//        return new TopicExchange(EXCHANGE, true, false);
//    }
//
//
//    @Bean
//    public Binding binding(Queue queue, TopicExchange exchange) {
//        return BindingBuilder
//                .bind(queue)
//                .to(exchange)
//                .with(ROUTING_KEY);
//    }
    @Bean(name = "connection-factory-dev")
    public ConnectionFactory connectionFactoryDev() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        return connectionFactory;
    }

    @Bean(name = "connection-factory-prod")
    @Primary
    public ConnectionFactory connectionFactoryProd() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5682);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        return connectionFactory;
    }


    @Bean
    public MessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

//    @Bean
//    public ConnectionFactoryContextWrapper contextWrapper( ConnectionFactory connectionFactory ) {
//        return new ConnectionFactoryContextWrapper(connectionFactory);
//    }

    @Bean("rabbit-template-dev")
    @Primary
    public RabbitTemplate rabbitTemplateDev( @Qualifier("connection-factory-dev") ConnectionFactory connectionFactory ) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(messageConverter());
        return rabbitTemplate;
    }

    @Bean("rabbit-template-prod")
    public RabbitTemplate rabbitTemplateProd( @Qualifier("connection-factory-prod") ConnectionFactory connectionFactory ) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(messageConverter());
        return rabbitTemplate;
    }

}

@Service
class RabbitMQService {

    private final RabbitTemplate rabbitTemplateProd;
    private final RabbitTemplate rabbitTemplateDev;

    RabbitMQService( @Qualifier("rabbit-template-prod") RabbitTemplate rabbitTemplateProd, @Qualifier("rabbit-template-dev") RabbitTemplate rabbitTemplateDev ) {
        this.rabbitTemplateProd = rabbitTemplateProd;
        this.rabbitTemplateDev = rabbitTemplateDev;
    }


    public void send( Object message, String type ) throws JsonProcessingException {

        if ( type.equals("dev") ) {
            rabbitTemplateDev.convertAndSend(RabbitMQConfig.EXCHANGE, "xt.request", new Message(new ObjectMapper().writeValueAsBytes(message)));
        } else if ( type.equals("prod") ) {
            rabbitTemplateProd.convertAndSend(RabbitMQConfig.EXCHANGE, "xt.request", new Message(new ObjectMapper().writeValueAsBytes(message)));
        }
    }

}