// Weyler Jorge dos Santos Silva - Cod. Pessoa: 1323740
// Professor: Wagner Cipriano da Silva - Desenvolvimento de Aplicações Distribuídas
// Esta atividade tem como proposito realizar o envio de uma mensagem atraves da
//implementacao de comunicacao com o broker Kafka por mei da ferramenta Event Hub do Microsoft Azure.

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class KafkaProducerExample {
    public static void main(String[] args) {
        // Configurações do Kafka Producer
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "puc-dad.servicebus.windows.net:9093");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://puc-dad.servicebus.windows.net;SharedAccessKeyName=aluno-dad;SharedAccessKey=Kds6a1hYMueVSSbu7bgiKXBpjBbzT4Kol+AEhNOt3FQ=\";");

        Producer<String, String> producer = new KafkaProducer<>(properties);

        // Mensagem a ser enviada
        String message = "{\"name\": \"Weyler Jorge dos Santos Silva\", \"login_id\": \"1323740@sga.pucminas.br\", \"group\": 1}";

        // Tópico de destino
        String topic = "dad-atividade-kafka";

        // Enviar a mensagem para o tópico
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("Mensagem enviada com sucesso para o tópico: " + metadata.topic());
            } else {
                System.err.println("Erro ao enviar a mensagem: " + exception.getMessage());
            }
        });

        // Fechar o produtor
        producer.close();
    }
}
