package peruncs.datagrid.storage.distributed.aeron.crashtest;

/** Shared class-path construction for Aeron crash-test child JVMs. */
final class ChildJava
{
	private ChildJava() { }

	static String classpath()
	{
		final String testPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
		final String modulePath = System.getProperty("jdk.module.path");
		return modulePath == null || modulePath.isBlank()
			? testPath : testPath + java.io.File.pathSeparator + modulePath;
	}
}
