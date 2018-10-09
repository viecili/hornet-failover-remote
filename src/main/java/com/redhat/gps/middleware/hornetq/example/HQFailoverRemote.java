package com.redhat.gps.middleware.hornetq.example;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;

public class HQFailoverRemote {

	private enum CONFIG_NAMES {
		HELP("help"), 
		VERBOSE("verbose"), 
		PRODUCER("producer"), 
		CONSUMER("consumer"), 
		DESTINATION("dest.name"), 
		FACTORY("factory.name"), 
		MESSAGE_TEXT("msg.text"), 
		MESSAGE_COUNT("msg.count"), 
		MESSAGE_INTERVAL("msg.interval"), 
		JNDI_PROVIDER_HOST("jndi.provider.host"), 
		JNDI_PROVIDER_PORT("jndi.provider.port")
		;
		private String property;

		CONFIG_NAMES(String property) {
			this.property = property;
		};

		@Override
		public String toString() {
			return property;
		}
	}

	private Boolean verbose, producer, consumer;

	private final Map<CONFIG_NAMES, Object> configMap = new LinkedHashMap<>(CONFIG_NAMES.values().length);

	private String jndiURL;

	private String destinationName;

	private String msg;

	private Integer amount;

	private String factoryName;

	private Long interval;

	public static void main(String[] args) throws Exception {
		if (System.getProperty(CONFIG_NAMES.HELP.toString()) != null) {
			printHelp();
			System.exit(0);
		}
		HQFailoverRemote instance = new HQFailoverRemote();
		instance.parseConfig();
		instance.printConfigMap();
		instance.run();
	}

	private void run() {
		if (producer) {
			if(verbose()) {
				System.out.println("PRODUCER Mode");
			}
			//produce(amount);
		} else if (consumer) {
			if(verbose()) {
				System.out.println("CONSUMER Mode");
			}
			//consume(amount);
		} else {
			System.out.println("producer or consumer must be specified");
			printHelp();
		}
	}

	private boolean verbose() {
		if (verbose == null) {
			verbose = System.getProperty(CONFIG_NAMES.VERBOSE.toString()) != null;
			configMap.put(CONFIG_NAMES.VERBOSE, verbose);
		}
		return verbose;
	}

	@SuppressWarnings({ "unchecked" })
	private <T> T getConfig(CONFIG_NAMES config, T defaultValue) {
		T value = defaultValue;
		if (configMap.containsKey(config)) {
			value = (T) configMap.get(config);
			if (verbose()) {
				System.out.println("config " + config + " from cache, value=" + value);
			}
		} else {
			value = (T) System.getProperties().getOrDefault(config.toString(), defaultValue);
			configMap.put(config, value);
			if (verbose()) {
				System.out.println("config " + config + " to cache, value=" + value);
			}
		}
		return (T) value;
	}

	private void printConfigMap() {
		System.out.println("Configuration: ");
		for (CONFIG_NAMES config : new TreeSet<>(Arrays.asList(CONFIG_NAMES.values()))) {
			Object value = configMap.get(config);
			if (value != null || verbose()) {
				System.out.println("\t" + config.toString() + ("".equals(value) ? "" : "=") + value);
			}
		}
	}

	private void parseConfig() throws Exception {
		verbose();
		jndiURL = "remote://" + getConfig(CONFIG_NAMES.JNDI_PROVIDER_HOST, "localhost") + ":"
				+ getConfig(CONFIG_NAMES.JNDI_PROVIDER_PORT, "1099");
		destinationName = getConfig(CONFIG_NAMES.DESTINATION, "/queue/TEST");
		producer = getConfig(CONFIG_NAMES.PRODUCER, null) != null;
		consumer = getConfig(CONFIG_NAMES.CONSUMER, null) != null;
		amount = getConfig(CONFIG_NAMES.MESSAGE_COUNT, 10);
		amount = (amount == -1 ? Integer.MAX_VALUE : amount);
		msg = getConfig(CONFIG_NAMES.MESSAGE_TEXT, "JMS Text Message #");
		interval = getConfig(CONFIG_NAMES.MESSAGE_INTERVAL, 500L);
		factoryName = getConfig(CONFIG_NAMES.FACTORY, "/RemoteConnectionFactory");

	}

	private static void printHelp() {
		System.out.println("usage: java -D<parameter>=<value> -jar jar_file_name.jar");
		System.out.println("\tparameters:");
		for (CONFIG_NAMES config : CONFIG_NAMES.values()) {
			System.out.println("\t\t" + config.toString());
		}

	}

	protected void produce() throws Exception {
/* */
		Connection connection = null;

		InitialContext initialContext = null;

		try {

			Properties properties = new Properties();
			properties.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");
			properties.put(Context.PROVIDER_URL, jndiURL);
			initialContext = new InitialContext(properties);
			Destination destination;

			// Step 2. Look up the JMS resources from JNDI
			destination = (Destination) initialContext.lookup(destinationName);
			ConnectionFactory connectionFactory = (ConnectionFactory) initialContext.lookup(factoryName);

			// Step 3. Create a JMS Connection
			connection = connectionFactory.createConnection();

			// Step 4. Create a *non-transacted* JMS Session with auto acknwoledgement
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			// Step 5. Start the connection to ensure delivery occurs
			connection.start();

			// Step 6. Create a JMS MessageProducer and a MessageConsumer
			MessageProducer producer = session.createProducer(destination);

			// Step 7. Send some messages to server #1, the live server
			for (int i = 0; i < amount; i++) {
				TextMessage message = session.createTextMessage(msg + i);
				producer.send(message);
				System.out.println("Sent message: " + message.getText());
				Thread.sleep(interval);
			}
		} finally {
			// Be sure to close our resources!

			if (connection != null) {
				connection.close();
			}

			if (initialContext != null) {
				initialContext.close();
			}
		} 
		/* */
	}
}
