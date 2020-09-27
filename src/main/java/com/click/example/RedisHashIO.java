package com.click.example;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisHashIO {

    private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

    public static RedisHashIO.Write write() {
        return (new com.click.example.AutoValue_RedisHashIO_Write.Builder()).setConnectionConfiguration(RedisConnectionConfiguration.create()).build();
    }

    @AutoValue
    public abstract static class Write extends PTransform<PCollection<KV<String, KV<String, String>>>, PDone> {
        public Write() {
        }

        @Nullable
        abstract RedisConnectionConfiguration connectionConfiguration();

        @Nullable
        abstract Long expireTime();

        abstract RedisHashIO.Write.Builder toBuilder();

        public RedisHashIO.Write withEndpoint(String host, int port) {
            Preconditions.checkArgument(host != null, "host can not be null");
            Preconditions.checkArgument(port > 0, "port can not be negative or 0");
            return this.toBuilder().setConnectionConfiguration(this.connectionConfiguration().withHost(host).withPort(port)).build();
        }

        public RedisHashIO.Write withAuth(String auth) {
            Preconditions.checkArgument(auth != null, "auth can not be null");
            return this.toBuilder().setConnectionConfiguration(this.connectionConfiguration().withAuth(auth)).build();
        }

        public RedisHashIO.Write withTimeout(int timeout) {
            Preconditions.checkArgument(timeout >= 0, "timeout can not be negative");
            return this.toBuilder().setConnectionConfiguration(this.connectionConfiguration().withTimeout(timeout)).build();
        }

        public RedisHashIO.Write withConnectionConfiguration(RedisConnectionConfiguration connection) {
            Preconditions.checkArgument(connection != null, "connection can not be null");
            return this.toBuilder().setConnectionConfiguration(connection).build();
        }

        public RedisHashIO.Write withExpireTime(Long expireTimeMillis) {
            Preconditions.checkArgument(expireTimeMillis != null, "expireTimeMillis can not be null");
            Preconditions.checkArgument(expireTimeMillis > 0L, "expireTimeMillis can not be negative or 0");
            return this.toBuilder().setExpireTime(expireTimeMillis).build();
        }

        public PDone expand(PCollection<KV<String, KV<String, String>>> input) {
            Preconditions.checkArgument(this.connectionConfiguration() != null, "withConnectionConfiguration() is required");
            input.apply(ParDo.of(new RedisHashIO.Write.WriteFn(this)));
            return PDone.in(input.getPipeline());
        }

        private static class WriteFn extends DoFn<KV<String, KV<String, String>>, Void>{
            private static final int DEFAULT_BATCH_SIZE = 1000;
            private final RedisHashIO.Write spec;
            private transient Jedis jedis;
            private transient Pipeline pipeline;
            private int batchCount;

            public WriteFn(RedisHashIO.Write spec) {
                this.spec = spec;
            }

            @Setup
            public void setup() {
                this.jedis = this.spec.connectionConfiguration().connect();
            }

            @StartBundle
            public void startBundle() {
                this.pipeline = this.jedis.pipelined();
                this.pipeline.multi();
                this.batchCount = 0;
            }

            @ProcessElement
            public void processElement(DoFn<KV<String, KV<String, String>>, Void>.ProcessContext c) {
                KV<String, KV<String, String>> record = c.element();

                writeRecord(record);

                batchCount++;

                if (batchCount >= DEFAULT_BATCH_SIZE) {
                    pipeline.exec();
                    pipeline.sync();
                    pipeline.multi();
                    batchCount = 0;
                    LOG.debug("Batch Write Complete and pipeline Flushed");
                }
                LOG.debug("Record Processed...");
            }

            private void writeRecord(KV<String, KV<String, String>> record) {
                String hashKey = record.getKey();
                KV<String, String> hashValue = record.getValue();
                String fieldKey = hashValue.getKey();
                String value = hashValue.getValue();

                pipeline.hset(hashKey, fieldKey, value);

            }


            @FinishBundle
            public void finishBundle() {
                if (this.pipeline.isInMulti()) {
                    this.pipeline.exec();
                    this.pipeline.sync();
                }

                this.batchCount = 0;
            }

            @Teardown
            public void teardown() {
                this.jedis.close();
            }
        }

        @com.google.auto.value.AutoValue.Builder
        abstract static class Builder {
            Builder() {
            }

            abstract RedisHashIO.Write.Builder setConnectionConfiguration(RedisConnectionConfiguration connectionConfiguration);

            abstract RedisHashIO.Write.Builder setExpireTime(Long expireTimeMillis);

            abstract RedisHashIO.Write build();
        }
    }
}
