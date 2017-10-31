/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.pulsar;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.pulsar.PulsarErrorCode.PULSAR_SPLIT_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Pulsar specific record set. Returns a cursor for a topic which iterates over a Pulsar partition segment.
 */
public class PulsarRecordSet
        implements RecordSet
{
    private static final Logger log = Logger.get(PulsarRecordSet.class);

    private static final int PULSAR_READ_BUFFER_SIZE = 100_000;
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final PulsarSplit split;
    private final PulsarClientManager clientManager;

    private final RowDecoder keyDecoder;
    private final RowDecoder messageDecoder;
    private final Map<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoders;
    private final Map<DecoderColumnHandle, FieldDecoder<?>> messageFieldDecoders;

    private final List<DecoderColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    private final Set<FieldValueProvider> globalInternalFieldValueProviders;

    PulsarRecordSet(PulsarSplit split,
                    PulsarClientManager clientManager,
            List<DecoderColumnHandle> columnHandles,
            RowDecoder keyDecoder,
            RowDecoder messageDecoder,
            Map<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoders,
            Map<DecoderColumnHandle, FieldDecoder<?>> messageFieldDecoders)
    {
        this.split = requireNonNull(split, "split is null");

        this.globalInternalFieldValueProviders = ImmutableSet.of(
            PulsarInternalFieldDescription.PARTITION_ID_FIELD.forLongValue(split.getPartitionId()),
            PulsarInternalFieldDescription.SEGMENT_START_FIELD.forByteValue(split.getStart().toByteArray()),
            PulsarInternalFieldDescription.SEGMENT_END_FIELD.forByteValue(split.getEnd().toByteArray()));

        this.clientManager = requireNonNull(clientManager, "clientManager is null");

        this.keyDecoder = requireNonNull(keyDecoder, "rowDecoder is null");
        this.messageDecoder = requireNonNull(messageDecoder, "rowDecoder is null");
        this.keyFieldDecoders = requireNonNull(keyFieldDecoders, "keyFieldDecoders is null");
        this.messageFieldDecoders = requireNonNull(messageFieldDecoders, "messageFieldDecoders is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();

        for (DecoderColumnHandle handle : columnHandles) {
            typeBuilder.add(handle.getType());
        }

        this.columnTypes = typeBuilder.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new PulsarRecordCursor();
    }

    public class PulsarRecordCursor
            implements RecordCursor
    {
        private long totalBytes;
        private long totalMessages;
        private final Reader reader = clientManager.getReader(split.getServiceUrl(), split.getTopicName(), split.getStart(), PULSAR_READ_BUFFER_SIZE);
        private Message message;

        private FieldValueProvider[] fieldValueProviders;

        PulsarRecordCursor()
        {
        }

        @Override
        public long getCompletedBytes()
        {
            return totalBytes;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return columnHandles.get(field).getType();
        }

        @Override
        public boolean advanceNextPosition()
        {
            try {
                if (reader.hasReachedEndOfTopic()) {
                    return false;
                }
                message = reader.readNext();
            }
            catch (PulsarClientException e) {
                throw new PrestoException(PULSAR_SPLIT_ERROR, "Failed to read next message");
            }
            return message != null && !message.getMessageId().equals(split.getEnd()) && nextRow(message);
        }

        private boolean nextRow(Message message)
        {
            totalBytes += message.getData().length;
            totalMessages++;
            byte[] keyData = message.hasKey() ? message.getKey().getBytes() : EMPTY_BYTE_ARRAY;
            byte[] messageData = message.getData();

            Set<FieldValueProvider> fieldValueProviders = new HashSet<>();

            fieldValueProviders.addAll(globalInternalFieldValueProviders);
            fieldValueProviders.add(PulsarInternalFieldDescription.SEGMENT_COUNT_FIELD.forLongValue(totalMessages));
            fieldValueProviders.add(PulsarInternalFieldDescription.MESSAGE_ID_FIELD.forByteValue(message.getMessageId().toByteArray()));
            fieldValueProviders.add(PulsarInternalFieldDescription.MESSAGE_FIELD.forByteValue(messageData));
            fieldValueProviders.add(PulsarInternalFieldDescription.MESSAGE_LENGTH_FIELD.forLongValue(messageData.length));
            fieldValueProviders.add(PulsarInternalFieldDescription.KEY_FIELD.forByteValue(keyData));
            fieldValueProviders.add(PulsarInternalFieldDescription.KEY_LENGTH_FIELD.forLongValue(keyData.length));
            fieldValueProviders.add(PulsarInternalFieldDescription.KEY_CORRUPT_FIELD.forBooleanValue(keyDecoder.decodeRow(keyData, null, fieldValueProviders, columnHandles, keyFieldDecoders)));
            fieldValueProviders.add(PulsarInternalFieldDescription.MESSAGE_CORRUPT_FIELD.forBooleanValue(messageDecoder.decodeRow(messageData, null, fieldValueProviders, columnHandles, messageFieldDecoders)));

            this.fieldValueProviders = new FieldValueProvider[columnHandles.size()];

            // If a value provider for a requested internal column is present, assign the
            // value to the internal cache. It is possible that an internal column is present
            // where no value provider exists (e.g. the '_corrupt' column with the DummyRowDecoder).
            // In that case, the cache is null (and the column is reported as null).
            for (int i = 0; i < columnHandles.size(); i++) {
                for (FieldValueProvider fieldValueProvider : fieldValueProviders) {
                    if (fieldValueProvider.accept(columnHandles.get(i))) {
                        this.fieldValueProviders[i] = fieldValueProvider;
                        break; // for(InternalColumnProvider...
                    }
                }
            }

            return true; // Advanced successfully.
        }

        @SuppressWarnings("SimplifiableConditionalExpression")
        @Override
        public boolean getBoolean(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, boolean.class);
            return isNull(field) ? false : fieldValueProviders[field].getBoolean();
        }

        @Override
        public long getLong(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, long.class);
            return isNull(field) ? 0L : fieldValueProviders[field].getLong();
        }

        @Override
        public double getDouble(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, double.class);
            return isNull(field) ? 0.0d : fieldValueProviders[field].getDouble();
        }

        @Override
        public Slice getSlice(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, Slice.class);
            return isNull(field) ? Slices.EMPTY_SLICE : fieldValueProviders[field].getSlice();
        }

        @Override
        public Object getObject(int field)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNull(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            return fieldValueProviders[field] == null || fieldValueProviders[field].isNull();
        }

        private void checkFieldType(int field, Class<?> expected)
        {
            Class<?> actual = getType(field).getJavaType();
            checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
        }

        @Override
        public void close()
        {
            try {
                reader.close();
            }
            catch (IOException ignored) {
            }
        }
    }
}
