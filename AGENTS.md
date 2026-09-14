Review @docs/aeron-clustering-integration-plan.md for the  genral outline of Aeron Eclipse datagrid implemtnation


## General code rules - items to investigate or fix:

1. Use all modern Java features for the java version speccified in the build framerok of the project, but no preview features.
2. Good OOP design - do the Java entities accurately map to expected APIs and domain concepts? Are Java entities and methods properly named, reflected their purpose?
3. Functional gaps and architectural design issues.
4. Opportunities to further simplify both the design and the code, make it DRY, cleaner and re-use as much as possible!
5. Signs of overengineering. Ask if this feature/code/functionality is really needed, remove or simplify it.
6. Ask yourself - how would you have approached or implemented this differently - and pursue the better approach?
7. Single-use methods that could be inlined. Java entities with 1-2 static methods that should be folded into stronger entities.
8. Proper use of AutoClosable with  try/catch resources managemnt in Java. Exceptiong handlig in genral, sallowed exceptions.
9. Minimal Java entinties and methods visibility surface (do not use "public" without reason)
10. Proper package structure - do not throw everything in one big package.
11. Avoid Java reflection unless absolutely necessary.
12. Any security gaps.
13. Any performance issues.
14. Any threading, races, deadlocks, TOCTOU and data corruption issues.
15. Robustness in face of network issues, configurable retrys.
16. Proper exception design, handling, propagation and reporting.
17. Correct and informative javadocs, including javadocs for packages (package-info.java) and modules (module-info.java)
18. Addd ample junit test and simulation coverage, especially since clustering is inolved. 
19. Have we looked at the Aeron examples and cookbook for best practices? Does the implementation follow them?
20. Use of Agrona and Eclipse Serializer and Eclipse Store utils  (LockedExecutor, StripeLockedExecutor) as much as possible ?
21. Eclipse Datagrid constraints are strictly obeyed: 1-writer/N-reader nodes . Kafka and Aeron are equal implementations (plug-ins), both observable.
22. Memory inefficiencies when packing data in Aeron CBE and Eclipse Serializer. Both formats use memory mapped files/ off-the-heap apis, so we want to avoid allocating objects (even temporary) on the JVM heap.
23. Make sure embedded Lucene and JVector indexes are tested and part of the implementation. 
24. If you notice issues with the legacy Kafka implementation, they should be.
25. Javadocs at all levels - module, package and individual Java entities. Use simple anrrative, less jargon, first sentence is the most important.



### If asked for review only
1. Do not run builds or junit tests - your task is only to do code analysis and review.
2. Do not modify any code - this is just a static analysis request.
3. Do not mention what is done correctly and well - there is no value in it.
4. When reporting you findings, be very specific and always offer your detailed recommendations for what and how you woudl do it instead.


### If asked to change code: 
Change the code aggressively, no code or data backward compatibility required! 

## Git policy
Do not perform ANY git mutation under any circumstences, do not lose code! Do git mutations only when asked explicitly, then use short, 1-liner commit mesaages.

## External libraries (`GITHUB_ROOT`)

`GITHUB_ROOT` is a **required** environment variable on each developer's
machine. It points to the directory containing `owner/repository` checkouts,
for example:

```bash
export GITHUB_ROOT="$HOME/projects/github"
```
Here are the reference source code directories to borrow from:

`$GITHUB_ROOT/apache/kafka`
`$GITHUB_ROOT/aeron-io`
`$GITHUB_ROOT/eclipse-store`
`$GITHUB_ROOT/eclipse-serializer`
`$GITHUB_ROOT/bhf/aeron-cache`


Use xberg to navigate the code and the documentation!
