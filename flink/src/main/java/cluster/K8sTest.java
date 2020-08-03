package cluster;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.internal.KubeConfigUtils;

import java.io.IOException;

public class K8sTest{
	public static void main(String[] args) throws IOException{

//		Config config = new ConfigBuilder().withMasterUrl("http://10.160.82.21:8080/").build();
//		KubernetesClient client = new DefaultKubernetesClient(config);
//		System.out.println(client.getNamespace());
//		System.out.println(client.getVersion());
//		System.out.println(client.services());
//		System.out.println(client.storage());
//		System.out.println(client.network());
//		System.out.println(client.pods());

		String kubeconfigContents = "apiVersion: v1\n" +
		                            "kind: ConfigMap\n" +
		                            "metadata:\n" +
		                            "  name: flink-config\n" +
		                            "  labels:\n" +
		                            "    app: flink\n" +
		                            "data:\n" +
		                            "  flink-conf.yaml: |+\n" +
		                            "    jobmanager.rpc.address: flink-jobmanager\n" +
		                            "    taskmanager.numberOfTaskSlots: 1\n" +
		                            "    blob.server.port: 6124\n" +
		                            "    jobmanager.rpc.port: 6123\n" +
		                            "    taskmanager.rpc.port: 6122\n" +
		                            "    jobmanager.heap.size: 1024m\n" +
		                            "    taskmanager.memory.process.size: 1024m\n" +
		                            "  log4j.properties: |+\n" +
		                            "    log4j.rootLogger=INFO, file\n" +
		                            "    log4j.logger.akka=INFO\n" +
		                            "    log4j.logger.org.apache.kafka=INFO\n" +
		                            "    log4j.logger.org.apache.hadoop=INFO\n" +
		                            "    log4j.logger.org.apache.zookeeper=INFO\n" +
		                            "    log4j.appender.file=org.apache.log4j.FileAppender\n" +
		                            "    log4j.appender.file.file=${log.file}\n" +
		                            "    log4j.appender.file.layout=org.apache.log4j.PatternLayout\n" +
		                            "    log4j.appender.file.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n\n" +
		                            "    log4j.logger.org.apache.flink.shaded.akka.org.jboss.netty.channel.DefaultChannelPipeline=ERROR, file";
//		io.fabric8.kubernetes.api.model.Config config = KubeConfigUtils.parseConfigFromString(
//				kubeconfigContents);
////		System.out.println(config);
//		KubernetesClient client = new DefaultKubernetesClient(config);
//		System.out.println(client.getNamespace());
//		System.out.println(client.getVersion());
//		System.out.println(client.services());
//		System.out.println(client.storage());
//		System.out.println(client.network());
//		System.out.println(client.pods());
	}
}
