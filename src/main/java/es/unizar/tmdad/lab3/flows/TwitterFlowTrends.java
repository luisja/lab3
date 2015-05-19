package es.unizar.tmdad.lab3.flows;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.AggregatorSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.amqp.Amqp;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.dsl.support.Consumer;
import org.springframework.social.twitter.api.Tweet;

@Configuration
@Profile("fanout")
public class TwitterFlowTrends extends TwitterFlowCommon {

	
	final static String TWITTER_FANOUT_A_QUEUE_NAME2 = "twitter_fanout_queue2";

	@Autowired
	FanoutExchange fanOutExchange;
	
	@Autowired
	RabbitTemplate rabbitTemplate;

	// Configuración RabbitMQ

	@Bean
	Queue aTwitterTrendsQueue() {
		return new Queue(TWITTER_FANOUT_A_QUEUE_NAME2, false);
	}

	@Bean
	Binding twitterTrendsBinding() {
		return BindingBuilder.bind(aTwitterTrendsQueue()).to(
				fanOutExchange);
	}

	// Flujo #1
	//
	// MessageGateway Twitter -(requestChannelTwitter)-> MessageEndpoint
	// RabbitMQ
	//

	@Bean
	public DirectChannel requestChannelTwitter() {
		return MessageChannels.direct().get();
	}

	@Bean
	public IntegrationFlow sendTrendsToRabbitMQ() {
		return IntegrationFlows.from(requestChannelTwitter())
				.filter((Object o) -> o instanceof Tweet)
				.aggregate(aggregationSpec(), null)
				.<List<Tweet>, List<Map.Entry<String, Integer>>>transform(tl -> {
					Map<String, Integer> mapTrends = new HashMap<String, Integer>();
					tl.stream().forEach(t -> {
						t.getEntities().getHashTags().forEach(ht-> {
							if(mapTrends.containsKey(ht.getText())) {
								mapTrends.put(ht.getText(), mapTrends.get(ht.getText()) + 1);
							} else {
								mapTrends.put(ht.getText(), 1);
							}
						});
					});
					Comparator<Map.Entry<String, Integer>> comparator = (Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) -> Integer.compare(o2.getValue(), o1.getValue());
					return mapTrends.entrySet().stream().sorted(comparator).limit(10).collect(Collectors.toList());
				})
				.handle("streamSendingService", "sendTrends").get();
	}
	
	private Consumer<AggregatorSpec> aggregationSpec() {
		return a -> a.correlationStrategy(m -> 1)
				.releaseStrategy(g -> g.size() == 1000)
				.expireGroupsUponCompletion(true);
	}

	// Flujo #2
	//
	// MessageEndpoint RabbitMQ -(requestChannelRabbitMQ)-> tareas ...
	//

	@Override
	@Bean
	public DirectChannel requestChannelRabbitMQ() {
		return MessageChannels.direct().get();
	}

	@Bean
	public AmqpInboundChannelAdapter amqpInbound() {
		SimpleMessageListenerContainer smlc = new SimpleMessageListenerContainer(
				rabbitTemplate.getConnectionFactory());
		smlc.setQueues(aTwitterTrendsQueue());
		return Amqp.inboundAdapter(smlc)
				.outputChannel(requestChannelRabbitMQ()).get();
	}
}
