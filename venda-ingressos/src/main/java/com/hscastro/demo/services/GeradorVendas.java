package com.hscastro.demo.services;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.hscastro.demo.domain.Venda;
import com.hscastro.demo.domain.VendaSerializer;

public class GeradorVendas {

	private static Random rand = new Random();
	private static long operacao = 0;
	private static BigDecimal valorIngresso = BigDecimal.valueOf(500);
	
	public static void main(String[] args) throws InterruptedException {
		
		//Instancia um obeto do tipo Properties
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VendaSerializer.class.getName());
		
		//Instancia um obeto do tipo KafkaProducer
		try(KafkaProducer<String, Venda> producer = new KafkaProducer<String, Venda>(properties)){
		
			while(true) {
			 	 //instancia um objeto do tipo venda
				 Venda venda = geraVenda();	
				 //instancia um objeto do tipo ProducerRecord
				 ProducerRecord<String, Venda> record = new ProducerRecord<String, Venda>("venda-ingresso", venda);
				 //chama o método send passando como parâmetro o ProducerRecord
				 producer.send(record);
				 
				 Thread.sleep(700);
				 
			}			
		}		
				
	}

	private static Venda geraVenda() {
		long cliente = rand.nextLong();
		int qtdIngressos = rand.nextInt();	
		return new Venda(operacao++, cliente, qtdIngressos, valorIngresso.multiply(BigDecimal.valueOf(qtdIngressos)));
	}
}
