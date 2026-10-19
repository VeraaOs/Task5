import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LogAnalysisSolution {

    public record LogEntry(
            String method,
            String path,
            int status,
            long responseBytes,
            long durationMillis
    ) {}

    public record LogFile(Path path) {}

    public record PathStatistics(
            long requests,
            long successful,
            long errors,
            long responseBytes,
            long totalDurationMillis,
            long maxDurationMillis
    ) {}

    public record Report(
            long totalRequests,
            long successfulRequests,
            long errorRequests,
            long responseBytes,
            long maxDurationMillis
    ) {}

    public static final class LogFileLines {
        private LogFileLines() {}

        public static Iterator<String> iterator(Path path) {
            try {
                BufferedReader reader = Files.newBufferedReader(
                        path,
                        StandardCharsets.UTF_8
                );

                return new Iterator<>() {
                    private String nextLine = readNext();
                    private boolean closed;

                    private String readNext() {
                        try {
                            String line = reader.readLine();
                            if (line == null) {
                                close();
                            }
                            return line;
                        } catch (IOException exception) {
                            close();
                            throw new RuntimeException(
                                    "Cannot read " + path,
                                    exception
                            );
                        }
                    }

                    private void close() {
                        if (closed) {
                            return;
                        }
                        closed = true;
                        try {
                            reader.close();
                        } catch (IOException exception) {
                            throw new RuntimeException(
                                    "Cannot close " + path,
                                    exception
                            );
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        return nextLine != null;
                    }

                    @Override
                    public String next() {
                        if (nextLine == null) {
                            throw new NoSuchElementException();
                        }
                        String result = nextLine;
                        nextLine = readNext();
                        return result;
                    }
                };
            } catch (IOException exception) {
                throw new RuntimeException(
                        "Cannot open " + path,
                        exception
                );
            }
        }
    }

    public static final class GenerateLogs {
        private static final String[] METHODS = {
                "GET", "POST", "PUT", "DELETE"
        };
        private static final String[] PATHS = {
                "/", "/index.html", "/login", "/api/users",
                "/api/orders", "/api/products", "/search", "/health"
        };
        private static final int[] STATUSES = {
                200, 200, 200, 201, 204,
                301, 400, 401, 404, 500
        };

        private GenerateLogs() {}

        public static void generate(
                Path outputDirectory,
                int fileCount,
                int linesPerFile
        ) throws IOException {
            if (fileCount < 0 || linesPerFile < 0) {
                throw new IllegalArgumentException(
                        "Counts must be non-negative"
                );
            }

            Files.createDirectories(outputDirectory);
            Random random = new Random(42);

            for (int fileId = 0; fileId < fileCount; fileId++) {
                Path file = outputDirectory.resolve(
                        "access-%03d.log".formatted(fileId)
                );

                try (BufferedWriter writer = Files.newBufferedWriter(
                        file,
                        StandardCharsets.UTF_8
                )) {
                    for (int line = 0; line < linesPerFile; line++) {
                        String method =
                                METHODS[random.nextInt(METHODS.length)];
                        String path =
                                PATHS[random.nextInt(PATHS.length)];
                        int status =
                                STATUSES[random.nextInt(STATUSES.length)];
                        long responseBytes =
                                100L + random.nextInt(100_000);
                        long durationMillis =
                                1L + random.nextInt(500);

                        writer.write(
                                "%s %s %d %d %d%n".formatted(
                                        method,
                                        path,
                                        status,
                                        responseBytes,
                                        durationMillis
                                )
                        );
                    }
                }
            }
        }
    }


    public static LogEntry parseLine(String line) {
        String[] fields = line.trim().split("\\s+");
        if (fields.length != 5) {
            throw new IllegalArgumentException(
                    "Expected 5 fields but got " + fields.length + ": " + line
            );
        }
        try {
            return new LogEntry(
                    fields[0],
                    fields[1],
                    Integer.parseInt(fields[2]),
                    Long.parseLong(fields[3]),
                    Long.parseLong(fields[4])
            );
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    "Cannot parse numbers in: " + line,
                    exception
            );
        }
    }

    public static boolean isSuccessful(LogEntry entry) {
        return entry.status() >= 200 && entry.status() <= 399;
    }

    public static Report buildReport(Map<String, PathStatistics> byPath) {
        long totalRequests = 0;
        long successfulRequests = 0;
        long errorRequests = 0;
        long responseBytes = 0;
        long maxDurationMillis = 0;
        for (PathStatistics stats : byPath.values()) {
            totalRequests += stats.requests();
            successfulRequests += stats.successful();
            errorRequests += stats.errors();
            responseBytes += stats.responseBytes();
            if (stats.maxDurationMillis() > maxDurationMillis) {
                maxDurationMillis = stats.maxDurationMillis();
            }
        }
        return new Report(
                totalRequests,
                successfulRequests,
                errorRequests,
                responseBytes,
                maxDurationMillis
        );
    }

    private static final class PathAccumulator {
        private long requests;
        private long successful;
        private long errors;
        private long responseBytes;
        private long totalDurationMillis;
        private long maxDurationMillis;

        void add(LogEntry entry) {
            requests++;
            if (isSuccessful(entry)) {
                successful++;
            } else {
                errors++;
            }
            responseBytes += entry.responseBytes();
            totalDurationMillis += entry.durationMillis();
            if (entry.durationMillis() > maxDurationMillis) {
                maxDurationMillis = entry.durationMillis();
            }
        }

        PathStatistics snapshot() {
            return new PathStatistics(
                    requests,
                    successful,
                    errors,
                    responseBytes,
                    totalDurationMillis,
                    maxDurationMillis
            );
        }
    }

    public interface StatisticsAccumulator {
        void add(String path, LogEntry entry);

        Map<String, PathStatistics> snapshot();
    }

    public static final class GlobalStatistics implements StatisticsAccumulator {
        private final Map<String, PathAccumulator> byPath = new HashMap<>();

        @Override
        public synchronized void add(String path, LogEntry entry) {
            PathAccumulator accumulator = byPath.get(path);
            if (accumulator == null) {
                accumulator = new PathAccumulator();
                byPath.put(path, accumulator);
            }
            accumulator.add(entry);
        }

        @Override
        public synchronized Map<String, PathStatistics> snapshot() {
            Map<String, PathStatistics> result = new HashMap<>();
            for (Map.Entry<String, PathAccumulator> entry : byPath.entrySet()) {
                result.put(entry.getKey(), entry.getValue().snapshot());
            }
            return Map.copyOf(result);
        }
    }

    public static final class ConcurrentStatistics implements StatisticsAccumulator {
        private final ConcurrentHashMap<String, PathAccumulator> byPath =
                new ConcurrentHashMap<>();

        @Override
        public void add(String path, LogEntry entry) {
            PathAccumulator accumulator = byPath.computeIfAbsent(
                    path,
                    key -> new PathAccumulator()
            );
            synchronized (accumulator) {
                accumulator.add(entry);
            }
        }

        @Override
        public Map<String, PathStatistics> snapshot() {
            Map<String, PathStatistics> result = new HashMap<>();
            for (Map.Entry<String, PathAccumulator> entry : byPath.entrySet()) {
                PathAccumulator accumulator = entry.getValue();
                synchronized (accumulator) {
                    result.put(entry.getKey(), accumulator.snapshot());
                }
            }
            return Map.copyOf(result);
        }
    }

    private static final class Counters {
        final AtomicLong filesProcessed = new AtomicLong();
        final AtomicLong linesRead = new AtomicLong();
        final AtomicLong validRecords = new AtomicLong();
        final AtomicLong damagedRecords = new AtomicLong();
        final AtomicLong readErrors = new AtomicLong();
        final AtomicInteger activeFiles = new AtomicInteger();
        final AtomicInteger maxActiveFiles = new AtomicInteger();

        void fileOpened() {
            int now = activeFiles.incrementAndGet();
            int observed = maxActiveFiles.get();
            while (now > observed) {
                if (maxActiveFiles.compareAndSet(observed, now)) {
                    break;
                }
                observed = maxActiveFiles.get();
            }
        }

        void fileClosed() {
            activeFiles.decrementAndGet();
        }
    }

    public record FileFailure(Path path, String reason) {}

    public record AnalysisResult(
            Report report,
            Map<String, PathStatistics> byPath,
            long filesProcessed,
            long linesRead,
            long validRecords,
            long damagedRecords,
            long readErrors,
            int maxActiveFiles,
            int activeFilesAtEnd,
            List<FileFailure> failedFiles,
            boolean stoppedEarly
    ) {}

    public static final class LogAnalyzer {
        private final List<Path> files;
        private final StatisticsAccumulator statistics;
        private final int workerCount;
        private final Semaphore semaphore;
        private final Counters counters = new Counters();
        private final Queue<FileFailure> failedFiles = new ConcurrentLinkedQueue<>();
        private volatile boolean stopRequested;

        public LogAnalyzer(
                List<Path> files,
                StatisticsAccumulator statistics,
                int workerCount,
                int semaphoreLimit
        ) {
            if (workerCount <= 0 || semaphoreLimit <= 0) {
                throw new IllegalArgumentException(
                        "workerCount and semaphoreLimit must be > 0"
                );
            }
            this.files = List.copyOf(files);
            this.statistics = statistics;
            this.workerCount = workerCount;
            this.semaphore = new Semaphore(semaphoreLimit);
        }

        public void requestStop() {
            stopRequested = true;
        }

        public boolean isStopRequested() {
            return stopRequested;
        }

        public AnalysisResult analyzeSequentially() {
            for (Path file : files) {
                processFile(file);
            }
            return buildResult();
        }

        public AnalysisResult analyzeParallel() {
            ExecutorService executor = Executors.newFixedThreadPool(workerCount);
            CountDownLatch finished = new CountDownLatch(files.size());
            try {
                for (Path file : files) {
                    executor.execute(() -> {
                        try {
                            processFile(file);
                        } finally {
                            finished.countDown();
                        }
                    });
                }
                finished.await();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                stopRequested = true;
            } finally {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    executor.shutdownNow();
                }
            }
            return buildResult();
        }

        private void processFile(Path file) {
            try {
                semaphore.acquire();
                counters.fileOpened();
                try {
                    Iterator<String> iterator = LogFileLines.iterator(file);
                    while (iterator.hasNext()) {
                        if (stopRequested) {
                            break;
                        }
                        String line = iterator.next();
                        counters.linesRead.incrementAndGet();
                        LogEntry entry;
                        try {
                            entry = parseLine(line);
                        } catch (RuntimeException damaged) {
                            counters.damagedRecords.incrementAndGet();
                            continue;
                        }
                        statistics.add(entry.path(), entry);
                        counters.validRecords.incrementAndGet();
                    }
                } finally {
                    counters.fileClosed();
                    semaphore.release();
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                counters.readErrors.incrementAndGet();
                failedFiles.add(new FileFailure(file, "interrupted"));
            } catch (RuntimeException exception) {
                counters.readErrors.incrementAndGet();
                failedFiles.add(new FileFailure(file, String.valueOf(exception.getMessage())));
            } finally {
                counters.filesProcessed.incrementAndGet();
            }
        }

        private AnalysisResult buildResult() {
            Map<String, PathStatistics> byPath = statistics.snapshot();
            return new AnalysisResult(
                    buildReport(byPath),
                    byPath,
                    counters.filesProcessed.get(),
                    counters.linesRead.get(),
                    counters.validRecords.get(),
                    counters.damagedRecords.get(),
                    counters.readErrors.get(),
                    counters.maxActiveFiles.get(),
                    counters.activeFiles.get(),
                    List.copyOf(failedFiles),
                    stopRequested
            );
        }
    }


    public static void generateSkewedLogs(
            Path outputDirectory,
            int fileCount,
            int linesPerFile
    ) throws IOException {
        Files.createDirectories(outputDirectory);
        Random random = new Random(42);
        for (int fileId = 0; fileId < fileCount; fileId++) {
            Path file = outputDirectory.resolve(
                    "skewed-%03d.log".formatted(fileId)
            );
            try (BufferedWriter writer = Files.newBufferedWriter(
                    file,
                    StandardCharsets.UTF_8
            )) {
                for (int line = 0; line < linesPerFile; line++) {
                    String path = random.nextInt(100) < 99 ? "/api/users" : "/other";
                    int status = random.nextInt(100) < 90 ? 200 : 500;
                    long responseBytes = 100L + random.nextInt(100_000);
                    long durationMillis = 1L + random.nextInt(500);
                    writer.write(
                            "GET %s %d %d %d%n".formatted(
                                    path,
                                    status,
                                    responseBytes,
                                    durationMillis
                            )
                    );
                }
            }
        }
    }

    private static List<Path> listLogFiles(Path directory) throws IOException {
        try (var stream = Files.list(directory)) {
            return stream
                    .filter(Files::isRegularFile)
                    .sorted()
                    .toList();
        }
    }

    private static void printResult(String title, AnalysisResult result) {
        System.out.println("--- " + title + " ---");
        System.out.println("файлов=" + result.filesProcessed()
                + " строк=" + result.linesRead()
                + " корректных=" + result.validRecords()
                + " повреждённых=" + result.damagedRecords()
                + " ошибок чтения=" + result.readErrors()
                + " maxActive=" + result.maxActiveFiles()
                + " activeAtEnd=" + result.activeFilesAtEnd()
                + " неудачных файлов=" + result.failedFiles().size()
                + " stoppedEarly=" + result.stoppedEarly());
        System.out.println("requests=" + result.report().totalRequests()
                + " successful=" + result.report().successfulRequests()
                + " errors=" + result.report().errorRequests()
                + " bytes=" + result.report().responseBytes()
                + " maxDuration=" + result.report().maxDurationMillis()
                + " urls=" + result.byPath().size());
    }

    private static double elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000d;
    }

    private static void measureLockStrategies(List<Path> files) {
        long globalStart = System.nanoTime();
        AnalysisResult globalResult = new LogAnalyzer(
                files, new GlobalStatistics(), 4, 4
        ).analyzeParallel();
        double globalMs = elapsedMillis(globalStart);

        long concurrentStart = System.nanoTime();
        AnalysisResult concurrentResult = new LogAnalyzer(
                files, new ConcurrentStatistics(), 4, 4
        ).analyzeParallel();
        double concurrentMs = elapsedMillis(concurrentStart);

        System.out.printf(
                "  global=%.1f ms, per-URL=%.1f ms (URLs=%d, requests=%d)%n",
                globalMs,
                concurrentMs,
                concurrentResult.byPath().size(),
                concurrentResult.report().totalRequests()
        );
        if (!globalResult.report().equals(concurrentResult.report())) {
            throw new AssertionError("Варианты статистики дали разный отчёт");
        }
    }

    private static void runExperiments() throws Exception {
        int fileCount = 16;
        int linesPerFile = 50_000;

        Path directory = Files.createTempDirectory("log-analysis-demo-");
        GenerateLogs.generate(directory, fileCount, linesPerFile);
        List<Path> files = listLogFiles(directory);
        System.out.println(
                "Сгенерировано " + files.size() + " файлов по "
                        + linesPerFile + " строк в " + directory
        );
        System.out.println();

        System.out.println("=== 1. Корректность: последовательный против параллельного ===");
        AnalysisResult sequentialResult = new LogAnalyzer(
                files, new ConcurrentStatistics(), 1, 4
        ).analyzeSequentially();
        AnalysisResult parallelResult = new LogAnalyzer(
                files, new ConcurrentStatistics(), 4, 4
        ).analyzeParallel();
        printResult("последовательно", sequentialResult);
        printResult("параллельно", parallelResult);
        if (!sequentialResult.report().equals(parallelResult.report())
                || !sequentialResult.byPath().equals(parallelResult.byPath())) {
            throw new AssertionError(
                    "Параллельный отчёт не совпал с последовательным"
            );
        }
        System.out.println("OK: отчёты совпали");
        System.out.println();

        System.out.println("=== 2. Число рабочих потоков: 1 против 4 (лимит 4) ===");
        for (int workers : new int[]{1, 4}) {
            long start = System.nanoTime();
            AnalysisResult result = new LogAnalyzer(
                    files, new ConcurrentStatistics(), workers, 4
            ).analyzeParallel();
            System.out.printf(
                    "workers=%d -> %.1f ms (maxActive=%d)%n",
                    workers,
                    elapsedMillis(start),
                    result.maxActiveFiles()
            );
        }
        System.out.println();

        System.out.println("=== 3. Лимит семафора: 1 против 4 (4 рабочих потока) ===");
        for (int limit : new int[]{1, 4}) {
            long start = System.nanoTime();
            AnalysisResult result = new LogAnalyzer(
                    files, new ConcurrentStatistics(), 4, limit
            ).analyzeParallel();
            System.out.printf(
                    "semaphore=%d -> %.1f ms (maxActive=%d)%n",
                    limit,
                    elapsedMillis(start),
                    result.maxActiveFiles()
            );
        }
        System.out.println();

        System.out.println("=== 4. Глобальная блокировка против блокировок по URL ===");
        System.out.println("данные с 8 URL:");
        measureLockStrategies(files);

        Path skewedDirectory = Files.createTempDirectory("log-analysis-skewed-");
        generateSkewedLogs(skewedDirectory, fileCount, linesPerFile);
        List<Path> skewedFiles = listLogFiles(skewedDirectory);
        System.out.println("данные, где 99% запросов к одному URL:");
        measureLockStrategies(skewedFiles);
        System.out.println();

        System.out.println("=== 5. Повреждённый файл и повреждённая строка ===");
        Path brokenFile = directory.resolve("broken.log");
        Files.createDirectory(brokenFile);
        List<Path> filesWithBroken = new ArrayList<>(files);
        filesWithBroken.add(brokenFile);
        AnalysisResult withBrokenFile = new LogAnalyzer(
                filesWithBroken, new ConcurrentStatistics(), 4, 4
        ).analyzeParallel();
        printResult("добавлен повреждённый файл", withBrokenFile);
        System.out.println("неудачные файлы: " + withBrokenFile.failedFiles());
        if (!withBrokenFile.report().equals(sequentialResult.report())) {
            throw new AssertionError("Повреждённый файл изменил общий отчёт");
        }
        System.out.println("OK: корректные файлы дали полный отчёт");

        Path damagedLineDirectory = Files.createTempDirectory("log-analysis-damaged-line-");
        GenerateLogs.generate(damagedLineDirectory, 1, linesPerFile);
        Path damagedLineFile = listLogFiles(damagedLineDirectory).get(0);
        Files.writeString(
                damagedLineFile,
                "BROKEN LINE WITHOUT FIELDS" + System.lineSeparator(),
                StandardCharsets.UTF_8,
                StandardOpenOption.APPEND
        );
        AnalysisResult withDamagedLine = new LogAnalyzer(
                List.of(damagedLineFile), new ConcurrentStatistics(), 4, 4
        ).analyzeParallel();
        printResult("добавлена повреждённая строка", withDamagedLine);
        if (withDamagedLine.damagedRecords() != 1
                || withDamagedLine.validRecords() != linesPerFile
                || !withDamagedLine.failedFiles().isEmpty()) {
            throw new AssertionError("Повреждённая строка обработана неверно");
        }
        System.out.println("OK: повреждённая строка пропущена, остальные строки учтены");
        System.out.println();

        System.out.println("=== 6. Досрочная остановка через volatile ===");
        LogAnalyzer stopper = new LogAnalyzer(
                files, new ConcurrentStatistics(), 4, 4
        );
        Thread controller = new Thread(() -> {
            try {
                Thread.sleep(20);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            stopper.requestStop();
        });
        controller.start();
        AnalysisResult stoppedResult = stopper.analyzeParallel();
        controller.join();
        System.out.println("stoppedEarly=" + stoppedResult.stoppedEarly()
                + " прочитано строк=" + stoppedResult.linesRead()
                + " из " + (long) fileCount * linesPerFile
                + " activeAtEnd=" + stoppedResult.activeFilesAtEnd()
                + " accepted=" + stoppedResult.validRecords()
                + " (частичный отчёт)");
        System.out.println();

        System.out.println("=== Готово ===");
    }

    private static void selfCheck() throws IOException {
        Path directory = Files.createTempDirectory("log-assignment-");
        GenerateLogs.generate(directory, 3, 7);

        int files = 0;
        int lines = 0;

        try (var paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                files++;
                Iterator<String> iterator = LogFileLines.iterator(path);

                while (iterator.hasNext()) {
                    String line = iterator.next();
                    String[] fields = line.trim().split("\\s+");
                    if (fields.length != 5) {
                        throw new AssertionError(
                                "Unexpected line: " + line
                        );
                    }
                    Integer.parseInt(fields[2]);
                    Long.parseLong(fields[3]);
                    Long.parseLong(fields[4]);
                    lines++;
                }
            }
        }

        if (files != 3 || lines != 21) {
            throw new AssertionError(
                    "Expected 3 files and 21 lines, got "
                            + files + " files and " + lines + " lines"
            );
        }

        System.out.println(
                "OK: generated " + files
                        + " files and read " + lines + " lines"
        );
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("generate")) {
            Path directory = Path.of(
                    args.length > 1 ? args[1] : "logs"
            );
            int fileCount = args.length > 2
                    ? Integer.parseInt(args[2])
                    : 20;
            int linesPerFile = args.length > 3
                    ? Integer.parseInt(args[3])
                    : 1_000;

            GenerateLogs.generate(
                    directory,
                    fileCount,
                    linesPerFile
            );

            System.out.println(
                    "Generated " + fileCount
                            + " files in " + directory
            );
            return;
        }

        if (args.length > 0 && args[0].equals("check")) {
            selfCheck();
            return;
        }

        if (args.length > 0 && args[0].equals("analyze")) {
            Path directory = Path.of(
                    args.length > 1 ? args[1] : "logs"
            );
            List<Path> files = listLogFiles(directory);
            AnalysisResult result = new LogAnalyzer(
                    files, new ConcurrentStatistics(), 4, 4
            ).analyzeParallel();
            printResult("analysis of " + directory, result);
            return;
        }

        if (args.length == 0) {
            runExperiments();
            return;
        }

        System.out.println(
                "Usage: java LogAnalysisSolution "
                        + "generate <directory> <files> <lines>"
        );
        System.out.println(
                "       java LogAnalysisSolution check"
        );
        System.out.println(
                "       java LogAnalysisSolution analyze <directory>"
        );
    }
}
