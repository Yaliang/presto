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
package com.facebook.presto.strato;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.io.CountingInputStream;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.strato.StratoErrorCode.STRATO_REQUEST_ERROR;
import static com.facebook.presto.strato.StratoErrorCode.STRATO_VALUE_INVALID;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class StratoRecordCursor
        implements RecordCursor
{
    private static TypeReference scanTypeReference = new TypeReference<List<List<Object>>>(){};
    private static TypeReference fetchTypeReference = new TypeReference<Object>(){};
    private final List<StratoColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;
    private final List<String> prefilledValues;

    private final Iterator<List<Object>> lines;
    private final long totalBytes;
    private final long nanoStart;
    private final long nanoEnd;

    private List<String> fields;
    private ObjectMapper mapper;

    public StratoRecordCursor(List<StratoColumnHandle> columnHandles, ByteSource byteSource, Optional<String> key, Map<Integer, String> prefilledValues)
    {
        this.columnHandles = columnHandles;
        this.prefilledValues = prefilledValues.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .collect(ImmutableList.toImmutableList());

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            StratoColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }

        this.mapper = new ObjectMapper();
        this.nanoStart = System.nanoTime();
        Iterator<List<Object>> lines = Collections.emptyIterator();
        long totalBytes = 0;
        try (CountingInputStream input = new CountingInputStream(byteSource.openStream())) {
            if (key.isPresent()) {
                Object value = mapper.readValue(input, fetchTypeReference);
                lines = Collections.singletonList(Arrays.asList(key.get(), value)).iterator();
            }
            else {
                List<List<Object>> records = mapper.readValue(input, scanTypeReference);
                lines = records.iterator();
            }

            totalBytes = input.getCount();
        }
        catch (FileNotFoundException ignored) {
            // ignored
        }
        catch (IOException e) {
            throw new PrestoException(STRATO_REQUEST_ERROR, "Failed to read Strato response: " + e.getMessage(), e);
        }
        this.nanoEnd = System.nanoTime();

        this.lines = lines;
        this.totalBytes = totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return nanoEnd - nanoStart;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!lines.hasNext()) {
            return false;
        }
        List<Object> line = lines.next();
        try {
            fields = ImmutableList.<String>builder()
                    .add(line.get(0).toString())
                    .add(mapper.writeValueAsString(line.get(1)))
                    .addAll(prefilledValues)
                    .build();
            System.out.println(fields.toString());
        }
        catch (JsonProcessingException e) {
            throw new PrestoException(STRATO_VALUE_INVALID, "Error to reprocess extracted value to json string", e);
        }

        return true;
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        return fields.get(columnIndex);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        return Long.parseLong(getFieldValue(field));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice(getFieldValue(field));
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
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }
}
