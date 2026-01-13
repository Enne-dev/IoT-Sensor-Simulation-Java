package com.example.iotspringboot;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

import com.amazonaws.services.iot.client.AWSIotMessage;
import com.amazonaws.services.iot.client.AWSIotMqttClient;
import com.amazonaws.services.iot.client.AWSIotQos;
import jakarta.annotation.PostConstruct;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileReader;
import java.security.Security;

@SpringBootApplication
@EnableScheduling
@RestController
public class IoTSpringBootApplication {
    private AWSIotMqttClient client;  // AWS client (when starting APP -> link)

    private static final String clientEndpoint = "a1u6nmfv3uv0e6-ats.iot.ap-northeast-1.amazonaws.com";
    private static final String clientId = "MySensor";
    private static final String certificateFile = "C:/Users/user/Desktop/iot-project/IoT-Sensor-Simulation-Java/src/main/resources/270fa4d7d83505d2cb7822d7bc394fdd6118e2d79d14830b5724dbdeef0b8a56-certificate.pem.crt";
    private static final String privateKeyFile = "C:/Users/user/Desktop/iot-project/IoT-Sensor-Simulation-Java/src/main/resources/270fa4d7d83505d2cb7822d7bc394fdd6118e2d79d14830b5724dbdeef0b8a56-private.pem.key";

    public static void main(String[] args) throws Exception {
        SpringApplication.run(IoTSpringBootApplication.class, args);
    }
    @PostConstruct
    public void init() throws Exception {
        Security.addProvider(new BouncyCastleProvider());

        KeyStore keyStore = loadKeyStore(certificateFile, privateKeyFile);

        client = new AWSIotMqttClient(clientEndpoint, clientId, keyStore, "password");
        client.connect();

        System.out.println("AWS IoT Connected!");
    }

    @Scheduled(fixedRate = 5000)  // 5秒たびにデータを作る ＆ 転送
    public void generateAndSendData() {
        Random random = new Random();
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
        double temperature = 20 + random.nextDouble() * 60; // 温度 20 ~ 80 度C
        double humidity = 30 + random.nextDouble() * 60; // 湿気 30 ~ 90 %
        double vibration = 0.1 + random.nextDouble() * 4.9; // 振動 0.1 ~ 5.0 mm/s^2

        String data = String.format("{\"timestamp\": \"%s\", \"temperature\": %.2f, \"humidity\": %.2f, \"vibration\": %.2f}",
                timestamp, temperature, humidity, vibration);
        System.out.println("Generated Data: " + data);

        String topic = "factory/sensor/data";
        AWSIotMessage msg = new AWSIotMessage(topic, AWSIotQos.QOS1, data);
        try {
            client.publish(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @GetMapping("/data")  // Web API: localhost:8080/dataでデータを確認
    public String getSensorData() {
        return generateData();
    }

    private static String generateData() {
        Random random = new Random();
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
        double temperature = 20 + random.nextDouble() * 60; // 温度 20 ~ 80 度C
        double humidity = 30 + random.nextDouble() * 60; // 湿気 30 ~ 90 %
        double vibration = 0.1 + random.nextDouble() * 4.9; // 振動 0.1 ~ 5.0 mm/s^2
        return String.format("{\"timestamp\": \"%s\", \"temperature\": %.2f, \"humidity\": %.2f, \"vibration\": %.2f}",
                timestamp, temperature, humidity, vibration);
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