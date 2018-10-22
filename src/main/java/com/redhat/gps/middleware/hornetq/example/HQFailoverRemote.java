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
		HELP("help", "[prints this help message]"), 
		DRY_RUN("dry", "[activate dry run mode]"), 
		VERBOSE("verbose", "[activate verbose output]"), 
		PRODUCER("producer", "[activate producer mode]"), 
		CONSUMER("consumer", "[activate consumer mode]"), 
		JNDI_PROVIDER_HOST("jndi.provider.host", "[the JNDI provider host (default: localhost)]"), 
		JNDI_PROVIDER_PORT("jndi.provider.port", "[the JNDI provider port (default: 4447)]"), 
		JNDI_PROVIDER_URL(Context.PROVIDER_URL, "[the full JNDI provider url, overrides 'jndi.provider.host' and 'jndi.provider.port']"), 
		DESTINATION("dest.name", "[the JNDI name of the JMS Destination (default: /queue/TEST)]"), 
		FACTORY("factory.name", "[the JNDI name of the JMS Connection Factory (default: /RemoteConnectionFactory)]"), 
		MESSAGE_TEXT("msg.text", "[the text prefix of the message when in producer mode (default: \"JMS Text Message #\")]"), 
		MESSAGE_COUNT("msg.count", "[amount of messages consumed/produced (default: 10 producer, -1/indefinite consumer )]"), 
		MESSAGE_INTERVAL("msg.interval", "[interval between messages, in ms (default:500ms)]"),
		;
		private String property, helpText;

		CONFIG_NAMES(String property) {
			this(property, "");
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

	private Connection connection = null;

	private InitialContext initialContext = null;

	public static void main(String[] args) throws Exception {
		if (System.getProperty(CONFIG_NAMES.HELP.toString()) != null) {
			printHelp();
			System.exit(0);
		}
		try {
			final HQFailoverRemote instance = new HQFailoverRemote();
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					try {
						instance.shutdown();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			});
			instance.parseConfig();
			instance.printConfigMap();
			instance.run();
		} catch (Exception e) {
			System.out.println("ERROR: " + e.getMessage());
			printHelp();
			e.printStackTrace(System.out);
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
		jndiURL = getConfig(CONFIG_NAMES.JNDI_PROVIDER_URL, null);
		if (jndiURL == null) {
			jndiURL = "remote://" + getConfig(CONFIG_NAMES.JNDI_PROVIDER_HOST, "localhost") + ":"
					+ getConfig(CONFIG_NAMES.JNDI_PROVIDER_PORT, "4447");
		}
		destinationName = getConfig(CONFIG_NAMES.DESTINATION, "jms/queue/TEST");
		producerMode = getConfig(CONFIG_NAMES.PRODUCER, null) != null;
		consumerMode = getConfig(CONFIG_NAMES.CONSUMER, null) != null;
		amount = Integer.valueOf(getConfig(CONFIG_NAMES.MESSAGE_COUNT, (producerMode ? "10" : "-1")));
		amount = (amount == -1 ? Integer.MAX_VALUE : amount);
		msg = getConfig(CONFIG_NAMES.MESSAGE_TEXT, "JMS Text Message #");
		interval = Long.valueOf(getConfig(CONFIG_NAMES.MESSAGE_INTERVAL, "500"));
		factoryName = getConfig(CONFIG_NAMES.FACTORY, "/RemoteConnectionFactory");
		System.out.println();
	}

	private static void printHelp() {
		System.out.println("usage: java -D<parameter>=<value> -jar jar_file_name.jar");
		System.out.println("\tparameters:");
		for (CONFIG_NAMES config : CONFIG_NAMES.values()) {

			System.out.println("\t\t" + String.format("%-25s", config.toString()) + config.helpText());
		}
		System.out.println();
	}

	protected void run() throws Exception {
		if (dryRun) {
			System.out.println("Dry Run");
			return;
		}
		if (!(producerMode ^ consumerMode)) {
			System.out.println("ERROR: producer or consumer must be specified (mutually exclusive)");
			printHelp();
			return;
		}

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
				if (verbose()) {
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
				if (verbose()) {
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
			shutdown();
		}
	}

	private void shutdown() throws Exception {
		try {
			System.out.println("Shutting down");
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
