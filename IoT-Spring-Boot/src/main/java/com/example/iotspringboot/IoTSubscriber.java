package com.example.iotspringboot;

import com.amazonaws.services.iot.client.AWSIotException;
import com.amazonaws.services.iot.client.AWSIotMessage;
import com.amazonaws.services.iot.client.AWSIotMqttClient;
import com.amazonaws.services.iot.client.AWSIotQos;
import com.amazonaws.services.iot.client.AWSIotTopic;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.FileInputStream;
import java.io.FileReader;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import weka.classifiers.functions.LinearRegression;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instances;

@Component
public class IoTSubscriber implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(IoTSubscriber.class);

    private static final String clientEndpoint = "a1u6nmfv3uv0e6-ats.iot.ap-northeast-1.amazonaws.com";
    private static final String clientId = "MySensorSubscriber";  // 기존 clientId와 구분
    private static final String certificateFile = "C:/Users/user/Desktop/iot-project/IoT-Spring-Boot/src/main/resources/270fa4d7d83505d2cb7822d7bc394fdd6118e2d79d14830b5724dbdeef0b8a56-certificate.pem.crt";
    private static final String privateKeyFile = "C:/Users/user/Desktop/iot-project/IoT-Spring-Boot/src/main/resources/270fa4d7d83505d2cb7822d7bc394fdd6118e2d79d14830b5724dbdeef0b8a56-private.pem.key";

    //DynamoDB tableの名前
    private static final String tableName = "SensorData";  // AWS DynamoDB table create

    private AWSIotMqttClient client;
    private final DynamoDbClient dynamoDbClient = DynamoDbClient.create();  // DynamoDB client
    private final ObjectMapper objectMapper = new ObjectMapper();  // JSON parsing

    // Digital Twin : Virtual Model
    private TwinModel digitalTwin = new TwinModel();

    @Override
    public void run(String... args) throws Exception {
        initializeClient();  // client init & connect
        subscribeToTopic("factory/sensor/data");  // 元のpublish topic subscribe ( AWS IoT core -> my thing / MQTT Test )
        logger.info("IoT Subscriber started and subscribed to topic.");
    }

    /**
     * AWS IoT MQTT client init & connect.
     * if connect failed -> retry
     */
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
                Thread.sleep(5000);  // 5s -> retry
                initializeClient();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                logger.error("Retry interrupted", ie);
            }
        }
    }

    /**
     * topic subscribe & msg handler setting
     * topic == MQTT topic ( "factory/sensor/data" )
     */
    private void subscribeToTopic(String topic) {
        try {
            AWSIotTopic iotTopic = new AWSIotTopic(topic, AWSIotQos.QOS1) {
                @Override
                public void onMessage(AWSIotMessage message) {
                    processMessage(message);  // 受けたメッセージを処理
                }
            };
            client.subscribe(iotTopic);
            logger.info("Subscribed to topic: {}", topic);
        } catch (AWSIotException e) {
            logger.error("Failed to subscribe to topic: {}", topic, e);
        }
    }

    /**
     * 受けたメッセージを処理: JSON parsing, 異常感知, DynamoDB save.
     * @param message 受けた AWS IoT メッセージ
     */
    private void processMessage(AWSIotMessage message) {
        String payload = message.getStringPayload();
        logger.info("Received message: {}", payload);

        try {
            // JSON 파싱
            JsonNode jsonNode = objectMapper.readTree(payload);
            String timestamp = jsonNode.get("timestamp").asText();
            double temperature = jsonNode.get("temperature").asDouble();
            double humidity = jsonNode.get("humidity").asDouble();
            double vibration = jsonNode.get("vibration").asDouble();

            // Digital Twin update (Virtual Model Simulation)
            digitalTwin.updateFromSensor(temperature, humidity, vibration);

            // ML + Digital Twin Integrate prediction
            performIntegratedPrediction(temperature, vibration);

            // DynamoDB save
            saveToDynamoDB(timestamp, temperature, humidity, vibration);
        } catch (Exception e) {
            logger.error("Error processing message: {}", payload, e);
        }
    }

    /**
     * parsed sensor data -> DynamoDB Save
     * @param timestamp タイムスタンプ
     * @param temperature 温度
     * @param humidity 湿度
     * @param vibration 振動
     */
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
            logger.error("Failed to save to DynamoDB: {}", e.getMessage(), e);
            e.printStackTrace();
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

    private class TwinModel {
        double currentTemp = 0.0;
        double currentHumidity = 0.0;
        double currentVibration = 0.0;
        double accumulatedWear = 0.0;  // 累積摩耗度(振動 -> simulation)
        double energyEfficiency = 100.0;  // エネルギー効率 (温度・湿度 -> simulation)

        //sensor data -> digital twin (update)
        public void updateFromSensor(double temp, double hum, double vib) {
            currentTemp = temp;
            currentHumidity = hum;
            currentVibration = vib;

            // logic: 振動で摩耗累積(factory DOWNTIME simulation)
            accumulatedWear += vib * 0.05;  // 任意係数（現実的に設定）

            // logic: エネルギー効率 (温度・湿度超過時の効率低下)
            if (temp > 30 || hum > 70) {
                energyEfficiency -= 1.0;  // 効率 down
            }

            logger.info("Digital Twin updated: Wear={}, Efficiency={}%", accumulatedWear, energyEfficiency);
        }

        public double getAccumulatedWear() {
            return accumulatedWear;
        }

        public double getEnergyEfficiency() {
            return energyEfficiency;
        }
    }

    //ML + Digital Twin 統合予測: 過去のデータ -> 統計/ML分析 -> ツイン状態との比較
    private void performIntegratedPrediction(double currentTemp, double currentVib) {
        try {
            // 過去のデータ(最近の10個のデータ （温度・振動）
            List<Double> recentTemps = queryRecentValues("temperature", 10);
            List<Double> recentVibs = queryRecentValues("vibration", 10);

            if (recentTemps.size() < 2 || recentVibs.size() < 2) {
                logger.info("Skipping ML prediction: Not enough data (temps: {}, vibs: {})", recentTemps.size(), recentVibs.size());
                return;  // データ 足りない場合 -> Skip
            }

            if (!recentTemps.isEmpty() && !recentVibs.isEmpty()) {
                // 統計的 異常を感知
                DescriptiveStatistics tempStats = new DescriptiveStatistics();
                recentTemps.forEach(tempStats::addValue);
                double tempMean = tempStats.getMean();
                double tempStdDev = tempStats.getStandardDeviation();
                double tempThreshold = tempMean + 2 * tempStdDev;

                if (currentTemp > tempThreshold) {
                    logger.warn("Statistical anomaly in temperature: Current {} > Threshold {}", currentTemp, tempThreshold);
                }

                //線形回帰予測（Weka：振動傾向予測）
                double predictedVib = predictTrend(recentVibs);  // 次の振動予測
                double vibDiff = Math.abs(currentVib - predictedVib);

                if (vibDiff > 2.0) {  // 差の しきい値
                    logger.warn("ML trend anomaly in vibration: Current {} vs Predicted {}", currentVib, predictedVib);
                }

                // Digital Twinと ML 統合: ツイン摩耗度 ＋ ML 予測 比較
                double twinWear = digitalTwin.getAccumulatedWear();
                double twinEfficiency = digitalTwin.getEnergyEfficiency();
                if (twinWear > 50 || twinEfficiency < 80 || vibDiff > 2.0) {
                    logger.warn("Integrated ML + Digital Twin alert: High wear ({}), Low efficiency ({}%), Vib anomaly ({})", twinWear, twinEfficiency, vibDiff);
                    // TODO: 実際のアラム（SNS/Email） または ダッシュボード UPDATE
                }
            }
        } catch (Exception e) {
            logger.error("Error in integrated prediction: {}", e.getMessage(), e);
            e.printStackTrace();
        }
    }

    /**
     * DynamoDBから最近の数をQUERY
     * @param attribute QUERY属性（ex、 温度）
     * @param limit 最近の数
     * @return 数のリスト
     */
    private List<Double> queryRecentValues(String attribute, int limit) {
        List<Double> values = new ArrayList<>();
        Map<String, String> expressionNames = new HashMap<>();
        expressionNames.put("#attr", attribute);  // attribute alias
        expressionNames.put("#ts", "timestamp");  // timestamp alias

        ScanRequest request = ScanRequest.builder()
                .tableName(tableName)
                .projectionExpression("#attr, #ts")  // alias
                .expressionAttributeNames(expressionNames)
                .limit(limit * 2)
                .build();

        try {
            ScanResponse response = dynamoDbClient.scan(request);
            List<Map<String, AttributeValue>> items = new ArrayList<>(response.items());
            items.sort((a, b) -> b.get("timestamp").s().compareTo(a.get("timestamp").s()));

            for (int i = 0; i < Math.min(limit, items.size()); i++) {
                Map<String, AttributeValue> item = items.get(i);
                if (item.containsKey(attribute)) {
                    values.add(Double.parseDouble(item.get(attribute).n()));
                }
            }
        } catch (DynamoDbException e) {
            logger.error("Failed to scan {}: {} (Error code: {})", attribute, e.getMessage(), e.awsErrorDetails().errorCode(), e);
            e.printStackTrace();
        }
        return values;
    }

    /**
     * Weka 線形回帰によるトレンド予測(最近のデータベースの次の値予測)
     * @param recentValues 最近の数のリスト
     * @return 予測の数
     * @throws Exception Weka エラー時
     */
    private double predictTrend(List<Double> recentValues) throws Exception {
        if (recentValues.size() < 2) {
            throw new Exception("Not enough data for linear regression: size " + recentValues.size());
        }
        ArrayList<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("time"));  // 入力： 時間のINDEX
        attributes.add(new Attribute("value"));  // 出力： 数 (温度・振動

        Instances data = new Instances("trend", attributes, recentValues.size());
        data.setClassIndex(1);

        for (int i = 0; i < recentValues.size(); i++) {
            DenseInstance instance = new DenseInstance(2);
            instance.setValue(0, i);
            instance.setValue(1, recentValues.get(i));
            data.add(instance);
        }

        LinearRegression model = new LinearRegression();
        model.buildClassifier(data);

        // 次の数を予測 (time = recentValues.size())
        DenseInstance next = new DenseInstance(2);
        next.setValue(0, recentValues.size());
        next.setDataset(data);

        return model.classifyInstance(next);
    }
}