package es.unizar.tmdad.lab3.flows;

import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.social.twitter.api.StreamListener;

// Se expone un bean con nombre "integrationStreamListener". Cada vez que un
// método
// es invocado se traduce en un mensaje a "requestChannel"
@MessagingGateway(name = "integrationStreamListener", defaultRequestChannel = "requestChannelTwitter")
interface TwitterGatewayListener extends StreamListener {

}