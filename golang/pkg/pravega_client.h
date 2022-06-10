/* Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved. */

#ifndef PRAVEGA_CLIENT_H
#define PRAVEGA_CLIENT_H

/* Generated with cbindgen:0.23.0 */

/* Warning, this file is autogenerated by cbindgen. Don't modify this manually. */

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct Slice Slice;

typedef struct StreamManager StreamManager;

typedef struct StreamReader StreamReader;

typedef struct StreamReaderGroup StreamReaderGroup;

typedef struct StreamScalingPolicy StreamScalingPolicy;

typedef struct StreamWriter StreamWriter;

typedef struct Buffer {
  uint8_t *ptr;
  uintptr_t len;
  uintptr_t cap;
} Buffer;

void free_buffer(struct Buffer buf);

extern void publishBridge(int32_t chan_id, uintptr_t obj_ptr, const char *errorMessage);

struct StreamManager *stream_manager_new(const char *uri, struct Buffer *err);

void stream_manager_destroy(struct StreamManager *manager);

bool stream_manager_create_scope(const struct StreamManager *manager,
                                 const char *scope,
                                 struct Buffer *err);

bool stream_manager_create_stream(const struct StreamManager *manager,
                                  const char *scope,
                                  const char *stream,
                                  int32_t initial_segments,
                                  struct Buffer *err);

struct StreamWriter *stream_writer_new(const struct StreamManager *manager,
                                       const char *scope,
                                       const char *stream,
                                       uintptr_t max_inflight_events,
                                       struct Buffer *err);

void stream_writer_destroy(struct StreamWriter *writer);

struct StreamReaderGroup *stream_reader_group_new(const struct StreamManager *manager,
                                                  const char *reader_group,
                                                  const char *scope,
                                                  const char *stream,
                                                  bool read_from_tail,
                                                  struct Buffer *err);

void stream_reader_group_destroy(struct StreamReaderGroup *rg);

struct StreamScalingPolicy *fixed_scaling_policy(int32_t num);

void scaling_policy_destroy(struct StreamScalingPolicy *policy);

void stream_writer_write_event(struct StreamWriter *writer,
                               struct Buffer event,
                               struct Buffer routing_key,
                               struct Buffer *err);

void stream_writer_flush(struct StreamWriter *writer, struct Buffer *err);

struct StreamReader *stream_reader_group_create_reader(const struct StreamReaderGroup *reader_group,
                                                       const char *reader,
                                                       struct Buffer *err);

void stream_reader_destroy(struct StreamReader *reader);

void stream_reader_get_segment_slice(struct StreamReader *reader,
                                     int32_t chan_id,
                                     struct Buffer *err);

void segment_slice_destroy(struct Slice *slice);

void stream_reader_release_segment_slice(struct StreamReader *reader,
                                         struct Slice *slice,
                                         struct Buffer *err);

void segment_slice_next(struct Slice *slice, struct Buffer *event, struct Buffer *err);

#endif /* PRAVEGA_CLIENT_H */