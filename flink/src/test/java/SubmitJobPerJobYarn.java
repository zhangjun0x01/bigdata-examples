import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.FlinkException;
import org.apache.flink.yarn.YarnClusterDescriptor;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

/**
 * 1.11 之前，使用yarn per job 模式提交
 */
public class SubmitJobPerJobYarn{

//	public static void main(String[] args) throws FlinkException, ExecutionException, InterruptedException, ProgramInvocationException{
//		YarnClient yarnClient = YarnClient.createYarnClient();
//		YarnConfiguration yarnConfiguration = new YarnConfiguration();
//		yarnClient.init(yarnConfiguration);
//		yarnClient.start();
//
//		String configurationDirectory = "/Users/user/work/flink/conf";
//		Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);
//
////        FlinkYarnSessionCli cli = new FlinkYarnSessionCli(configuration, configurationDirectory, "y", "yarn");
//
//		YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
//				configuration,
//				yarnConfiguration,
//				configurationDirectory,
//				yarnClient,
//				false);
////        yarnClusterDescriptor.setLocalJarPath(new Path(""));
//		yarnClusterDescriptor.setLocalJarPath(new Path(
//				"/Users/user/work/flink/lib/flink-dist_2.12-1.9.0.jar"));
//		File flinkLibFolder = new File("/Users/user/work/flink/lib");
//		yarnClusterDescriptor.addShipFiles(Arrays.asList(flinkLibFolder.listFiles()));
//
////        JobGraph jobGraph = getJobGraph();
////        File testingJar = new File("/Users/user/work/flink/examples/streaming/TopSpeedWindowing.jar");
////
////        jobGraph.addJar(new org.apache.flink.core.fs.Path(testingJar.toURI()));
////
//		ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
//				.setMasterMemoryMB(1024)
//				.setTaskManagerMemoryMB(1024)
//				.setNumberTaskManagers(1)
//				.setSlotsPerTaskManager(1)
//				.createClusterSpecification();
//
//		RestClusterClient<ApplicationId> client = (RestClusterClient<ApplicationId>) yarnClusterDescriptor
//				.deploySessionCluster(clusterSpecification);
//
//
//
//		PackagedProgram prog = buildProgram(InputParams options)
//		client.run(prog,1);
////		CompletableFuture<JobSubmissionResult> future = client.submitJob(GetGraph.getJobGraph());
//
////		System.out.println(future.get());
//		System.out.println(client);
//
//
//        yarnClusterDescriptor.setName("myjob");
//        ClusterClient<ApplicationId> clusterClient = yarnClusterDescriptor.deployJobCluster(clusterSpecification,
//                                                                                            jobGraph,
//                                                                                            true);
//
//
//
//        ApplicationId applicationId = clusterClient.getClusterId();
//
//        final RestClusterClient<ApplicationId> restClusterClient = (RestClusterClient<ApplicationId>) clusterClient;
//
//        final CompletableFuture<JobResult> jobResultCompletableFuture = restClusterClient.requestJobResult(jobGraph.getJobID());
//
//        final JobResult jobResult = jobResultCompletableFuture.get();
//
//
//        System.out.println(applicationId);
//        System.out.println(jobResult);
//	}



	//    protected PackagedProgram buildProgram(InputParams options) {
//        String[] programArgs = options.getProgramArgs();
//        String jarFilePath = options.getJarFilePath();
//        List<URL> classpaths = options.getClasspaths();
//
//        if (jarFilePath == null) {
//            throw new IllegalArgumentException("The program JAR file was not specified.");
//        }
//
//        File jarFile = new File(jarFilePath);
//
//        // Check if JAR file exists
//        if (!jarFile.exists()) {
//            throw new FileNotFoundException("JAR file does not exist: " + jarFile);
//        } else if (!jarFile.isFile()) {
//            throw new FileNotFoundException("JAR file is not a file: " + jarFile);
//        }
//
//        // Get assembler class
//        String entryPointClass = options.getEntryPointClass();
//
//        PackagedProgram program = entryPointClass == null ?
//                new PackagedProgram(jarFile, classpaths, programArgs) :
//                new PackagedProgram(jarFile, classpaths, entryPointClass, programArgs);
//
//        return null;
//    }


}
