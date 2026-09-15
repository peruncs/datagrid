package peruncs.datagrid.cluster.nodelibrary.node;

import peruncs.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Verifies fatal node errors remain under the embedding application's control. */
class GlobalErrorHandlingTest
{
	@Test
	void rethrowsRuntimeFailuresWithoutTerminatingTheJvm()
	{
		final RuntimeException failure = new IllegalStateException("boom");
		assertSame(failure, assertThrows(RuntimeException.class,
			() -> GlobalErrorHandling.handleFatalError(failure)));
	}

	@Test
	void wrapsCheckedFailuresWithoutTerminatingTheJvm()
	{
		final Exception failure = new Exception("boom");
		final NodelibraryException reported = assertThrows(NodelibraryException.class,
			() -> GlobalErrorHandling.handleFatalError(failure));
		assertSame(failure, reported.getCause());
	}
}
