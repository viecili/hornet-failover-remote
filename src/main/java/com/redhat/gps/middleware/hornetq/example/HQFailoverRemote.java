package com.redhat.gps.middleware.hornetq.example;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;

public class HQFailoverRemote {

	private enum CONFIG_NAMES {
		HELP("help","[prints this help message]"), 
		DRY_RUN("dry","[activate dry run mode]"),
		VERBOSE("verbose","[activate verbose output]"), 
		PRODUCER("producer","[activate producer mode]"), 
		CONSUMER("consumer", "[activate producer mode]"), 
		JNDI_PROVIDER_HOST("jndi.provider.host"), 
		JNDI_PROVIDER_PORT("jndi.provider.port"),
		DESTINATION("dest.name"), 
		FACTORY("factory.name"), 
		MESSAGE_TEXT("msg.text"), 
		MESSAGE_COUNT("msg.count","[amount of messages consumed/produced (default: 10 producer, -1/indefinite consumer )]"), 
		MESSAGE_INTERVAL("msg.interval","[interval between messages, in ms (default:500ms)]"), 
		;
		private String property, helpText;

		CONFIG_NAMES(String property) {
			this(property,"");
		};
		
		CONFIG_NAMES(String property, String helpText) {
			this.property = property;
			this.helpText = helpText;
		};

		@Override
		public String toString() {
			return property;
		}
		
		public String helpText() {
			return helpText;
		}
	}

	private Boolean verbose, dryRun, producerMode, consumerMode = false;

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
		try {
			HQFailoverRemote instance = new HQFailoverRemote();
			instance.parseConfig();
			instance.printConfigMap();
			instance.run();
		} catch (Exception e) {
			System.out.println("ERROR"+e.getMessage());
			printHelp();
			throw e;
		}
		System.out.println("Done.");
		System.exit(0);
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
		dryRun = getConfig(CONFIG_NAMES.DRY_RUN, null) != null;
		jndiURL = "remote://" + getConfig(CONFIG_NAMES.JNDI_PROVIDER_HOST, "localhost") + ":"
				+ getConfig(CONFIG_NAMES.JNDI_PROVIDER_PORT, "1099");
		destinationName = getConfig(CONFIG_NAMES.DESTINATION, "/queue/TEST");
		producerMode = getConfig(CONFIG_NAMES.PRODUCER, null) != null;
		consumerMode = getConfig(CONFIG_NAMES.CONSUMER, null) != null;
		amount = getConfig(CONFIG_NAMES.MESSAGE_COUNT, (producerMode ? 10 : -1));
		amount = (amount == -1 ? Integer.MAX_VALUE : amount);
		msg = getConfig(CONFIG_NAMES.MESSAGE_TEXT, "JMS Text Message #");
		interval = getConfig(CONFIG_NAMES.MESSAGE_INTERVAL, 500L);
		factoryName = getConfig(CONFIG_NAMES.FACTORY, "/RemoteConnectionFactory");
		System.out.println();
	}

	private static void printHelp() {
		System.out.println("usage: java -D<parameter>=<value> -jar jar_file_name.jar");
		System.out.println("\tparameters:");
		for (CONFIG_NAMES config : CONFIG_NAMES.values()) {
			
			System.out.println("\t\t" + String.format("%-25s",config.toString()) + config.helpText());
		}
		System.out.println();
	}
	
	protected void run() throws Exception {
		if(dryRun) {
			System.out.println("Dry Run");
			return;
		}
		if (!(producerMode ^ consumerMode)) {
			System.out.println("ERROR: producer or consumer must be specified (mutually exclusive)");
			printHelp();
			return;
		}
		Connection connection = null;

		InitialContext initialContext = null;

		try {

			Properties properties = new Properties();
			properties.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");
			properties.put(Context.PROVIDER_URL, jndiURL);
			initialContext = new InitialContext(properties);
			Destination destination;

			destination = (Destination) initialContext.lookup(destinationName);
			ConnectionFactory connectionFactory = (ConnectionFactory) initialContext.lookup(factoryName);

			connection = connectionFactory.createConnection();

			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			connection.start();

			if (producerMode) {
				if(verbose()) {
					System.out.println("PRODUCER Mode");
				}
				
				MessageProducer producer = session.createProducer(destination);

				for (int i = 0; i < amount; i++) {
					try {
						TextMessage message = session.createTextMessage(msg + i);
						producer.send(message);
						System.out.println("Sent message: " + message.getText());
					} catch (Exception e) {
						e.printStackTrace();
					}
					Thread.sleep(interval);
				}
			} else if (consumerMode) {
				if(verbose()) {
					System.out.println("CONSUMER Mode");
				}

				MessageConsumer consumer = session.createConsumer(destination);

				for (int i = 0; i < amount; i++) {
					try {
						TextMessage message = (TextMessage) consumer.receive();
						System.out.println("Received message: " + message.getText());
					} catch (Exception e) {
						e.printStackTrace();
					}
					Thread.sleep(interval);
				}
			} 

		} finally {

			if (connection != null) {
				connection.close();
			}

			if (initialContext != null) {
				initialContext.close();
			}
		} 
	}
}
