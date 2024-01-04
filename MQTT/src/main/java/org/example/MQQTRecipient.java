package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MQQTRecipient {
    private static final String BROKER_URL = "tcp://broker.hivemq.com:1883";
    private static final String TOPIC = "test/topic";
    private static final String CLIENT_ID = MqttClient.generateClientId();
    private static final String DB_URL = "jdbc:mysql://localhost:3306/mqtt";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "password";

    public void run() {

        try {
            MqttClient client = new MqttClient(BROKER_URL, CLIENT_ID);
            client.setCallback(new MqttCallback() {
                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    String payload = new String(message.getPayload());
                    System.out.println("Message: " + payload);
                    writeToDatabase(DB_URL, DB_USER, DB_PASSWORD, payload);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
                    System.out.println("Delivery completed");
                }

                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("Connection lost: " + cause.getMessage());
                }
            });

            client.connect();
            client.subscribe(TOPIC);
            Thread.sleep(60000);
            client.disconnect();
            client.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void writeToDatabase(String url, String user, String password, String data) {
        try (Connection con = DriverManager.getConnection(url, user, password)) {
            String query = "INSERT INTO message (message) VALUES (?)";
            try (PreparedStatement pst = con.prepareStatement(query)) {
                pst.setString(1, data);
                pst.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Couldn't write to db");
        }
    }
}
