package com.example.iotspringboot;

import com.amazonaws.services.iot.client.AWSIotException;
import com.amazonaws.services.iot.client.AWSIotMqttClient;
import com.amazonaws.services.iot.client.AWSIotMessage;
import com.amazonaws.services.iot.client.AWSIotQos;
import com.amazonaws.services.iot.client.AWSIotTopic;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.io.FileInputStream;
import java.io.FileReader;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Component
public class IotSubscriber implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(IotSubscriber.class);

    private static final String clientEndpoint = "a1u6nmfv3uv0e6-ats.iot.ap-northeast-1.amazonaws.com";
    private static final String clientId = "MySensorSubscriber";
    private static final String certificateFile = "C:/Users/user/Desktop/iot-project/IoT-Sensor-Simulation-Java/src/main/resources/270fa4d7d83505d2cb7822d7bc394fdd6118e2d79d14830b5724dbdeef0b8a56-certificate.pem.crt";
    private static final String privateKeyFile = "C:/Users/user/Desktop/iot-project/IoT-Sensor-Simulation-Java/src/main/resources/270fa4d7d83505d2cb7822d7bc394fdd6118e2d79d14830b5724dbdeef0b8a56-private.pem.key";

    // DynamoDB tableの名前
    private static final String tableName = "SensorData";  // AWS DynamoDB table
    private AWSIotMqttClient client;
    private final DynamoDbClient dynamoDbClient = DynamoDbClient.create();  // DynamoDB client
    private final ObjectMapper objectMapper = new ObjectMapper();  // JSON parsing

    @Override
    public void run(String... args) throws Exception {
        initializeClient();  // client init & connect
        subscribeToTopic("factory/sensor/data");  // Topic Subscribing
        logger.info("IoT Subscriber started and subscribed to topic.");
    }

    //AWS IoT MQTT Client init & connect
    //if connect failed -> retry
    private void initializeClient() {
        try {
            Security.addProvider(new BouncyCastleProvider());
            KeyStore keyStore = loadKeyStore(certificateFile, privateKeyFile);

            client = new AWSIotMqttClient(clientEndpoint, clientId, keyStore, "password");
            client.connect();
            logger.info("Connected to AWS IoT endpoint: {}", clientEndpoint);
        } catch (Exception e) {
            logger.error("Failed to connect to AWS IoT. Retrying in 5 seconds...", e);
            try {
                Thread.sleep(5000);  // 5초 대기 후 재시도 (무한 루프 방지 위해 실제로는 카운터 추가 추천)
                initializeClient();  // 재귀 재시도
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                logger.error("Retry interrupted", ie);
            }
        }
    }

    //topic subscribe & message handler setting
    //@param topic -> subscribing -> MQTT topic (factory/sensor/data)
    private void subscribeToTopic(String topic) {
        try {
            AWSIotTopic iotTopic = new AWSIotTopic(topic, AWSIotQos.QOS1) {
                @Override
                public void onMessage(AWSIotMessage message) {
                    processMessage(message);  // 수신 메시지 처리
                }
            };
            client.subscribe(iotTopic);
            logger.info("Subscribed to topic: {}", topic);
        } catch (AWSIotException e) {
            logger.error("Failed to subscribe to topic: {}", topic, e);
        }
    }

    //受信されたMQTT msgを処理 -> JSON parsing, 異常感知, DynamoDB Save
    //@param message -> 受信 -> AWS IoT msg
    private void processMessage(AWSIotMessage message) {
        String payload = message.getStringPayload();
        logger.info("Received message: {}", payload);

        try {
            //JSON parsing
            JsonNode jsonNode = objectMapper.readTree(payload);
            String timestamp = jsonNode.get("timestamp").asText();
            double temperature = jsonNode.get("temperature").asDouble();
            double humidity = jsonNode.get("humidity").asDouble();
            double vibration = jsonNode.get("vibration").asDouble();

            //異常 感知 (ex : 温度が80度Cを超えたり振動が4.0を超えたら警告)
            if (temperature > 80 || vibration > 4.0) {
                logger.warn("Anomaly detected: High temperature ({}) or vibration ({}) at {}", temperature, vibration, timestamp);
                // TODO: 실제 알림 (SNS, Email) 추가 가능
            }

            //DynamoDBに保存
            saveToDynamoDB(timestamp, temperature, humidity, vibration);
        } catch (Exception e) {
            logger.error("Error processing message: {}", payload, e);
        }
    }

    //parsingされた センサーのデーターをDynamoDBに保存
    private void saveToDynamoDB(String timestamp, double temperature, double humidity, double vibration) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("timestamp", AttributeValue.builder().s(timestamp).build());
        item.put("temperature", AttributeValue.builder().n(String.valueOf(temperature)).build());
        item.put("humidity", AttributeValue.builder().n(String.valueOf(humidity)).build());
        item.put("vibration", AttributeValue.builder().n(String.valueOf(vibration)).build());

        PutItemRequest request = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build();

        try {
            dynamoDbClient.putItem(request);
            logger.info("Data saved to DynamoDB: timestamp={}, temperature={}, humidity={}, vibration={}", timestamp, temperature, humidity, vibration);
        } catch (DynamoDbException e) {
            logger.error("Failed to save to DynamoDB", e);
        }
    }

    private static KeyStore loadKeyStore(String certPath, String keyPath) throws Exception {
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        X509Certificate cert = (X509Certificate) factory.generateCertificate(new FileInputStream(certPath));

        PEMParser pemParser = new PEMParser(new FileReader(keyPath));
        Object object = pemParser.readObject();
        JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");
        java.security.KeyPair keyPair = converter.getKeyPair((org.bouncycastle.openssl.PEMKeyPair) object);
        pemParser.close();

        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setCertificateEntry("alias", cert);
        keyStore.setKeyEntry("alias", keyPair.getPrivate(), "password".toCharArray(), new java.security.cert.Certificate[]{cert});

        return keyStore;
    }
}