Flume File Dump interceptor
===========================

This interceptor dumps incoming events to a binary file in the local file system.

The dump filename can be configured using the "dump.filename" property on the interceptor.
See [FileDumpInterceptorTest](./src/test/scala/org/keedio/flume/interceptor/FileDumpInterceptorTest) for an useage example.

Only useful for debugging purposes.
