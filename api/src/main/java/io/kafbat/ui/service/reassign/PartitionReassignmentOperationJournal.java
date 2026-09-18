package io.kafbat.ui.service.reassign;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kafbat.ui.config.ClusterOperationsProperties;
import io.kafbat.ui.exception.PartitionReassignmentConflictException;
import jakarta.annotation.PreDestroy;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Service;

@Service
public class PartitionReassignmentOperationJournal implements Closeable {

  private static final int FORMAT_VERSION = 1;

  private final ObjectMapper objectMapper;
  private final Path root;
  private FileChannel lockChannel;
  private FileLock lock;

  public PartitionReassignmentOperationJournal(
      ObjectMapper objectMapper,
      ClusterOperationsProperties properties) {
    this.objectMapper = objectMapper;
    this.root = properties.getOperationJournalPath().toAbsolutePath().normalize();
  }

  public synchronized PartitionReassignmentOperation create(
      PartitionReassignmentOperation operation) {
    ensureOwned();
    var contents = read(operation.clusterName());
    var existing = contents.operations().stream()
        .filter(candidate -> candidate.operationId().equals(operation.operationId()))
        .findFirst();
    if (existing.isPresent()) {
      if (existing.get().requestFingerprint().equals(operation.requestFingerprint())) {
        return existing.get();
      }
      throw new PartitionReassignmentConflictException(
          "Partition reassignment operation ID is already used: " + operation.operationId());
    }
    var operations = new ArrayList<>(contents.operations());
    operations.add(operation);
    write(new JournalContents(FORMAT_VERSION, operation.clusterName(), operations));
    return operation;
  }

  public synchronized PartitionReassignmentOperation update(
      PartitionReassignmentOperation operation) {
    ensureOwned();
    var contents = read(operation.clusterName());
    var operations = new ArrayList<>(contents.operations());
    var replaced = false;
    for (var index = 0; index < operations.size(); index++) {
      if (operations.get(index).operationId().equals(operation.operationId())) {
        operations.set(index, operation);
        replaced = true;
        break;
      }
    }
    if (!replaced) {
      throw new IllegalArgumentException(
          "Unknown partition reassignment operation: " + operation.operationId());
    }
    write(new JournalContents(FORMAT_VERSION, operation.clusterName(), operations));
    return operation;
  }

  public synchronized Optional<PartitionReassignmentOperation> find(
      String clusterName,
      String operationId) {
    ensureOwned();
    return read(clusterName).operations().stream()
        .filter(operation -> operation.operationId().equals(operationId))
        .findFirst();
  }

  public synchronized List<PartitionReassignmentOperation> listUnresolved() {
    ensureOwned();
    try (var files = Files.list(root)) {
      return files
          .filter(path -> path.getFileName().toString().endsWith(".json"))
          .flatMap(path -> read(path).operations().stream())
          .filter(operation -> !operation.status().isTerminal())
          .sorted(Comparator
              .comparing(PartitionReassignmentOperation::clusterName)
              .thenComparing(PartitionReassignmentOperation::operationId))
          .toList();
    } catch (IOException exception) {
      throw new UncheckedIOException("Unable to list reassignment operations", exception);
    }
  }

  public synchronized List<PartitionReassignmentOperation> list(String clusterName) {
    ensureOwned();
    return read(clusterName).operations();
  }

  private JournalContents read(String clusterName) {
    var path = journalPath(clusterName);
    if (!Files.exists(path)) {
      return new JournalContents(FORMAT_VERSION, clusterName, List.of());
    }
    var contents = read(path);
    if (!contents.clusterName().equals(clusterName)) {
      throw new IllegalStateException("Partition reassignment journal cluster mismatch");
    }
    return contents;
  }

  private JournalContents read(Path path) {
    try {
      var contents = objectMapper.readValue(path.toFile(), JournalContents.class);
      if (contents.version() != FORMAT_VERSION) {
        throw new IllegalStateException(
            "Unsupported partition reassignment journal version: " + contents.version());
      }
      return contents;
    } catch (IOException exception) {
      throw new UncheckedIOException("Unable to read reassignment operation journal", exception);
    }
  }

  private void write(JournalContents contents) {
    var destination = journalPath(contents.clusterName());
    Path temporary = null;
    try {
      temporary = Files.createTempFile(root, destination.getFileName().toString(), ".tmp");
      objectMapper.writeValue(temporary.toFile(), contents);
      try (var channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
        channel.force(true);
      }
      Files.move(
          temporary,
          destination,
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException exception) {
      throw new UncheckedIOException("Unable to write reassignment operation journal", exception);
    } finally {
      if (temporary != null) {
        try {
          Files.deleteIfExists(temporary);
        } catch (IOException ignored) {
          // The destination is authoritative; stale temp files are ignored on reads.
        }
      }
    }
  }

  private Path journalPath(String clusterName) {
    return root.resolve(sha256(clusterName) + ".json");
  }

  private void ensureOwned() {
    if (lock != null && lock.isValid()) {
      return;
    }
    try {
      Files.createDirectories(root);
      lockChannel = FileChannel.open(
          root.resolve(".lock"),
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE);
      lock = lockChannel.tryLock();
      if (lock == null) {
        closeChannel();
        throw alreadyInUse();
      }
    } catch (OverlappingFileLockException exception) {
      closeChannel();
      throw new IllegalStateException(
          "Partition reassignment operation journal is already in use", exception);
    } catch (IOException exception) {
      closeChannel();
      throw new IllegalStateException(
          "Unable to acquire the partition reassignment operation journal", exception);
    }
  }

  private static IllegalStateException alreadyInUse() {
    return new IllegalStateException(
        "Partition reassignment operation journal is already in use");
  }

  private static String sha256(String value) {
    try {
      return HexFormat.of().formatHex(
          MessageDigest.getInstance("SHA-256")
              .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException("SHA-256 is unavailable", exception);
    }
  }

  @Override
  @PreDestroy
  public synchronized void close() {
    if (lock != null) {
      try {
        lock.release();
      } catch (IOException ignored) {
        // Closing the channel below releases the operating-system lock as a fallback.
      }
      lock = null;
    }
    closeChannel();
  }

  private void closeChannel() {
    if (lockChannel != null) {
      try {
        lockChannel.close();
      } catch (IOException ignored) {
        // There is no recovery action for a failed close.
      }
      lockChannel = null;
    }
  }

  private record JournalContents(
      int version,
      String clusterName,
      List<PartitionReassignmentOperation> operations) {

    private JournalContents {
      operations = List.copyOf(operations);
    }
  }
}
