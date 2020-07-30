import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;

import com.tcyl.dam.datacenter.reaco.model.jobcommon.InputParams;

import java.io.FileNotFoundException;

public abstract class ParserUserJar{



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
