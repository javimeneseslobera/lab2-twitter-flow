package es.unizar.tmdad.lab2.configuration;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.social.twitter.api.StreamListener;
import org.springframework.social.twitter.api.Tweet;
import org.springframework.web.util.HtmlUtils;

import es.unizar.tmdad.lab2.domain.MyTweet;
import es.unizar.tmdad.lab2.domain.TargetedTweet;
import es.unizar.tmdad.lab2.service.TwitterLookupService;

@Configuration
@EnableIntegration
@IntegrationComponentScan
@ComponentScan
public class TwitterFlow {

	@Bean
	public DirectChannel requestChannel() {
		return new DirectChannel();
	}

	@Autowired
	private TwitterLookupService lookupService;

	// Tercer paso
	// Los mensajes se leen de "requestChannel" y se envian al método "sendTweet"
	// del
	// componente "streamSendingService"
	@Bean
	public IntegrationFlow sendTweet() {
		//
		// CAMBIOS A REALIZAR:
		//
		// Usando Spring Integration DSL
		//
		// Filter --> asegurarnos que el mensaje es un Tweet
		// Transform --> convertir un Tweet en un TargetedTweet con tantos tópicos como
		// coincida
		// Split --> dividir un TargetedTweet con muchos tópicos en tantos
		// TargetedTweet como tópicos haya
		// Transform --> señalar el contenido de un TargetedTweet
		//
		IntegrationFlowBuilder integrationFlowBuilder = IntegrationFlows.from(requestChannel());
		integrationFlowBuilder = integrationFlowBuilder.filter(t -> t instanceof Tweet);
		integrationFlowBuilder = integrationFlowBuilder.transform(Tweet.class, t -> TransformToTargetedTweet(t));
		integrationFlowBuilder = integrationFlowBuilder.split(TargetedTweet.class, t -> SplitTargetedTweet(t));
		integrationFlowBuilder = integrationFlowBuilder.transform(TargetedTweet.class, t -> PointOutTweetContent(t));
		return integrationFlowBuilder.handle("streamSendingService", "sendTweet").get();
	}

	private TargetedTweet TransformToTargetedTweet(Tweet t) {
		MyTweet tweet = new MyTweet(t);
		List<String> topics = lookupService.getQueries().stream().filter(key -> t.getText().contains(key))
				.collect(Collectors.toList());
		return new TargetedTweet(tweet, topics);
	}

	private List<TargetedTweet> SplitTargetedTweet(TargetedTweet t) {
		return t.getTargets().stream().map(key -> new TargetedTweet(t.getTweet(), key)).collect(Collectors.toList());
	}

	private TargetedTweet PointOutTweetContent(TargetedTweet t) {
		t.getTweet().setUnmodifiedText(t.getTweet().getUnmodifiedText().replaceAll(t.getFirstTarget(),
				"<b>" + HtmlUtils.htmlEscape(t.getFirstTarget()) + "</b>"));
		return t;
	}

}

// Segundo paso
// Los mensajes recibidos por este @MessagingGateway se dejan en el canal
// "requestChannel"
@MessagingGateway(name = "integrationStreamListener", defaultRequestChannel = "requestChannel")
interface MyStreamListener extends StreamListener {

}
