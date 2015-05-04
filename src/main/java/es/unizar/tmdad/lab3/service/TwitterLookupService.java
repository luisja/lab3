package es.unizar.tmdad.lab3.service;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.springframework.stereotype.Service;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

@Service
public class TwitterLookupService {

	private final ConcurrentMap<String, String> connections = new ConcurrentLinkedHashMap.Builder<String, String>()
			.maximumWeightedCapacity(10).build();

	public void search(String query) {
		connections.putIfAbsent(query, query);
	}
	
	public Set<String> getQueries() {
		return connections.keySet();
	}
	
}
