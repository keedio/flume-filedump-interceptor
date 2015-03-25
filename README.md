Flume File Dump interceptor
===========================

This interceptor dumps incoming events to rolling logs in the local file system.

The base name for log files and rolling options can be configured in the interceptor using the
following properties:

- dump.filename: full path and name of the log file
- dump.maxFileSize: max size for log files. When this size is reached, the log file will be rolled.
- dump.maxBackups: max number of backup logs to keep. Backup files will be named using the filename and the
sequence number. Example: /var/log/flume/dump.log.1 (where 1 is the most recent backup, 2 the second most recent, etc.).

The maxFileSize option can be specified in bytes, kilobytes, megabytes or gigabytes by suffixing a numeric
value with KB, MB and respectively GB.

Example of configuration:

```ini
# interceptor
a1.sources.r1.interceptors = i1
a1.sources.r1.interceptors.i1.type = org.keedio.flume.interceptor.FileDumpInterceptorBuilder

# Properties for rolling logs
a1.sources.r1.interceptors.i1.dump.filename = /var/log/flume/dump.log
a1.sources.r1.interceptors.i1.dump.maxFileSize = 10MB
a1.sources.r1.interceptors.i1.dump.maxBackups = 10
```

See [FileDumpInterceptorTest](./src/test/scala/org/keedio/flume/interceptor/FileDumpInterceptorTest) for an usage example.

Only useful for debugging purposes.
