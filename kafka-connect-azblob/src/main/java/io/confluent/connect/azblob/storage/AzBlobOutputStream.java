/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.azblob.storage;

import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;

/**
 * Output stream enabling multi-part uploads of Kafka records.
 *
 * The implementation has borrowed the general structure of Hadoop's implementation.
 */
public class AzBlobOutputStream extends OutputStream {
  private static final Logger log = LoggerFactory.getLogger(AzBlobOutputStream.class);
  private final String container;
  private final String key;
  private final int partSize;
  private final CloudBlobContainer azContainer;
  private boolean closed;
  private ByteBuffer buffer;
  private MultipartUpload multiPartUpload;

  public AzBlobOutputStream(String key, AzBlobSinkConnectorConfig conf, CloudBlobContainer azContainer) {
    this.azContainer = azContainer;
    this.container = conf.getContainerName();
    this.key = key;
    this.partSize = conf.getPartSize();
    this.closed = false;
    this.buffer = ByteBuffer.allocate(this.partSize);
    this.multiPartUpload = null;
    log.debug("Create AzBlobOutputStream for container '{}' key '{}'", container, key);
  }

  @Override
  public void write(int b) throws IOException {
    buffer.put((byte) b);
    if (!buffer.hasRemaining()) {
      uploadPart();
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || off > b.length || len < 0 || (off + len) > b.length || (off + len) < 0) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    if (buffer.remaining() < len) {
      int firstPart = buffer.remaining();
      buffer.put(b, off, firstPart);
      uploadPart();
      write(b, off + firstPart, len - firstPart);
    } else {
      buffer.put(b, off, len);
    }
  }

  private void uploadPart() throws IOException {
    uploadPart(partSize);
    buffer.clear();
  }

  private void uploadPart(int size) throws IOException {
    if (multiPartUpload == null) {
      log.debug("New multi-part upload for container '{}' key '{}'", container, key);
      multiPartUpload = newMultipartUpload();
    }

    try {
      multiPartUpload.uploadPart(new ByteArrayInputStream(buffer.array()), size);
    } catch (Exception e) {
      // TODO: elaborate on the exception interpretation. We might be able to retry.
      if (multiPartUpload != null) {
        multiPartUpload.abort();
        log.debug("Multipart upload aborted for container '{}' key '{}'.", container, key);
      }
      throw new IOException("Part upload failed: ", e.getCause());
    }
  }

  public void commit() throws IOException {
    if (closed) {
      log.warn("Tried to commit data for container '{}' key '{}' on a closed stream. Ignoring.", container, key);
      return;
    }

    try {
      if (buffer.hasRemaining()) {
        uploadPart(buffer.position());
      }
//      multiPartUpload.complete();
      log.debug("Upload complete for container '{}' key '{}'", container, key);
    } catch (Exception e) {
      log.error("Multipart upload failed to complete for container '{}' key '{}'", container, key);
      throw new DataException("Multipart upload failed to complete.", e);
    } finally {
      buffer.clear();
      multiPartUpload = null;
      close();
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
//    if (multiPartUpload != null) {
//      multiPartUpload.abort();
//      log.debug("Multipart upload aborted for container '{}' key '{}'.", container, key);
//    }
    super.close();
  }

//  private ObjectMetadata newObjectMetadata() {
//    ObjectMetadata meta = new ObjectMetadata();
//    return meta;
//  }

  private MultipartUpload newMultipartUpload() throws IOException {
    InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(container, key, newObjectMetadata());
    try {
      return new MultipartUpload(s3.initiateMultipartUpload(initRequest).getUploadId());
    } catch (AmazonClientException e) {
      // TODO: elaborate on the exception interpretation. If this is an AmazonServiceException,
      // there's more info to be extracted.
      throw new IOException("Unable to initiate MultipartUpload: " + e, e);
    }
  }

//  private class MultipartUpload {
//    private final String uploadId;
//    private final List<PartETag> partETags;
//
//    public MultipartUpload(String uploadId) {
//      this.uploadId = uploadId;
//      this.partETags = new ArrayList<>();
//      log.debug("Initiated multi-part upload for container '{}' key '{}' with id '{}'", container, key, uploadId);
//    }
//
//    public void uploadPart(ByteArrayInputStream inputStream, int partSize) {
//      int currentPartNumber = partETags.size() + 1;
//      UploadPartRequest request = new UploadPartRequest()
//                                            .withBucketName(container)
//                                            .withKey(key)
//                                            .withUploadId(uploadId)
//                                            .withInputStream(inputStream)
//                                            .withPartNumber(currentPartNumber)
//                                            .withPartSize(partSize)
//                                            .withGeneralProgressListener(progressListener);
//      log.debug("Uploading part {} for id '{}'", currentPartNumber, uploadId);
//      partETags.add(s3.uploadPart(request).getPartETag());
//    }
//
//    public void complete() {
//      log.debug("Completing multi-part upload for key '{}', id '{}'", key, uploadId);
//      CompleteMultipartUploadRequest completeRequest =
//          new CompleteMultipartUploadRequest(container, key, uploadId, partETags);
//      s3.completeMultipartUpload(completeRequest);
//    }
//
//    public void abort() {
//      log.warn("Aborting multi-part upload with id '{}'", uploadId);
//      try {
//        s3.abortMultipartUpload(new AbortMultipartUploadRequest(container, key, uploadId));
//      } catch (Exception e) {
//        // ignoring failure on abort.
//        log.warn("Unable to abort multipart upload, you may need to purge uploaded parts: ", e);
//      }
//    }
//  }

}
