Review @peruncs-cache/README.md, @peruncs-cluster/README.md and their module-info.java records for the general outline of the Aeron datagrid implementation

## General code rules - items to investigate or fix:

1. Use all modern Java features for the java version specified in the build framework, including preview features such as: Virtual threads, Patern matching, ScopedValue, StructuredTaskScope, LazyConstant
2. Follow good OOP design - map the Java entities map to expected APIs and domain concepts. Are Java entities and methods properly named, reflected
   on their purpose?
3. Prefer java records, immutable entities, modern functional style java code. Use Optional only for method input params.
4. Functional gaps and architectural design issues.
5. Opportunities to further simplify both the design and the code, make it DRY, cleaner and re-use as much as possible!
6. Signs of overengineering. Ask if this feature/code/functionality is really needed, remove or simplify it.
7. Ask yourself - how would you have approached or implemented this differently - and pursue the better approach?
8. Single-use methods that could be inlined. Java entities with 1-2 static methods that should be folded into stronger entities.
9. Proper use of AutoClosable with try/catch resources managemnt in Java. Exceptiong handlig in genral, sallowed exceptions.
10. Minimal Java entinties and methods visibility surface (do not use "public" without reason)
11. Proper package structure with package-info.java javadocs - do not throw everything in one big package.
12. Avoid Java reflection unless absolutely necessary.
13. Any security gaps.
14. Any performance issues.
15. Any threading, races, deadlocks, TOCTOU and data corruption issues.
16. Robustness in face of network issues, configurable retrys.
17. Proper exception design, handling, propagation and reporting.
18. Correct and informative javadocs, including javadocs for packages (package-info.java) and modules (module-info.java)
19. Addd ample junit test and simulation coverage, especially since clustering is inolved.
20. Have we looked at the Aeron examples and cookbook for best practices? Does the implementation follow them?
21. Use of Agrona and Eclipse Serializer and Eclipse Store utils (LockedExecutor, StripeLockedExecutor) as much as possible ?
22. Eclipse Datagrid constraints are strictly obeyed: 1-writer/N-reader nodes.
23. Memory inefficiencies when packing data in Aeron CBE and Eclipse Serializer. Both formats use memory mapped files/ off-the-heap apis, so we want
    to avoid allocating objects (even temporary) on the JVM heap.
24. Make sure embedded Lucene and JVector indexes are tested and part of the implementation.
25. Javadocs at all levels - module, package and individual Java entities. Use simple narrative suitable for humans, less jargon, first sentence is the most important. 
26. Avoid using fully qualified names 9FQN) where sesnible imports can make the code more compact and better to read.


### If asked for review only

1. Do not run builds or junit tests - your task is only to do code analysis and review.
2. Do not modify any code - this is just a static analysis request.
3. Do not mention what is done correctly and well - there is no value in it.
4. When reporting you findings, be very specific and always offer your detailed recommendations for what and how you woudl do it instead.

### If asked to change code:

Change the code aggressively, no code or data backward compatibility required!

## Git policy

Under any circumstances, Do not perform ANY git mutations, do not restore code form Git , do not lose code! 
Do git mutations only when asked explicitly, then use short, 1-liner commit mesaages.

## External libraries (`GITHUB_ROOT`)

`GITHUB_ROOT` is a **required** environment variable on each developer's
machine. It points to the directory containing `owner/repository` checkouts,
for example:

```bash
export GITHUB_ROOT="$HOME/projects/github"
```

Here are the reference source code directories to borrow from:

`$GITHUB_ROOT/aeron-io`
`$GITHUB_ROOT/eclipse-store`
`$GITHUB_ROOT/eclipse-serializer`
`$GITHUB_ROOT/bhf/aeron-cache`

