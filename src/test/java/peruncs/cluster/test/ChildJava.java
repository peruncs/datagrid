package peruncs.cluster.test;

import java.io.File;

/// Shared class-path construction for forked crash-test child JVMs.
///
/// Crash tests fork a child JVM to observe a real process failure, so both the
/// storage and node crash-test suites build the child class path through this
/// one helper.
public interface ChildJava {

    /// Returns the class path a forked crash-test child must be started with.
    ///
    /// @return test and module path joined for the child JVM
    static String classpath() {
        final String testPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        final String modulePath = System.getProperty("jdk.module.path");
        return modulePath == null || modulePath.isBlank()  ? testPath : testPath + File.pathSeparator + modulePath;
    }
}
