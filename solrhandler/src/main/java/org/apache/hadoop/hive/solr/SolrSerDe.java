/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.solr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class SolrSerDe extends AbstractSerDe {
    // Currently we support only int, float, double, boolean, string 
    // data types through SolrSerDe
    static final String HIVE_TYPE_DOUBLE = "double";
    static final String HIVE_TYPE_FLOAT = "float";
    static final String HIVE_TYPE_BOOLEAN = "boolean";
    static final String HIVE_TYPE_INT = "int";

    private final MapWritable cachedWritable = new MapWritable();

    private int fieldCount;
    private StructObjectInspector objectInspector;
    private List<String> columnNames;
    private String[] columnTypesArray;
    private List<Object> row;
    
    @Override
    public void initialize(final Configuration conf, final Properties tbl)
            throws SerDeException {

        String colNamesStr = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        String[] columnNamesArray = colNamesStr.split(",");  
        columnNames = Arrays.asList(colNamesStr.split(","));
        fieldCount = columnNamesArray.length;
        String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        columnTypesArray = columnTypeProperty.split(":");
 
        final List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(
                columnNamesArray.length);
        for (int i = 0; i < columnNamesArray.length; i++) {
            if (HIVE_TYPE_INT.equalsIgnoreCase(columnTypesArray[i])) {
                fieldOIs
                        .add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
            } else if (SolrSerDe.HIVE_TYPE_BOOLEAN.equalsIgnoreCase(columnTypesArray[i])) {
                fieldOIs
                        .add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
            } else if (SolrSerDe.HIVE_TYPE_FLOAT.equalsIgnoreCase(columnTypesArray[i])) {
                fieldOIs
                        .add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
            } else if (SolrSerDe.HIVE_TYPE_DOUBLE.equalsIgnoreCase(columnTypesArray[i])) {
                fieldOIs
                        .add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
            } else {
                // treat as string
                fieldOIs
                        .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            }
        }
        objectInspector = ObjectInspectorFactory
                .getStandardStructObjectInspector(columnNames, fieldOIs);
        row = new ArrayList<Object>(columnNamesArray.length);
    }

    @Override
    public Object deserialize(Writable wr) throws SerDeException {
        
        if (!(wr instanceof MapWritable)) {
            throw new SerDeException("Expected MapWritable, received "
                    + wr.getClass().getName());
        }
        
        final MapWritable input = (MapWritable) wr;
        final Text t = new Text();
        row.clear();

        for (int i = 0; i < fieldCount; i++) {
            t.set(columnNames.get(i));
            final Writable value = input.get(t);
            if (value != null && !NullWritable.get().equals(value)) {
                 if (HIVE_TYPE_INT.equalsIgnoreCase(columnTypesArray[i])) {
                    row.add(Double.valueOf(value.toString()).intValue());
                }else if (SolrSerDe.HIVE_TYPE_BOOLEAN.equalsIgnoreCase(columnTypesArray[i])) {
                    row.add(Boolean.valueOf(value.toString()));
                } else if (SolrSerDe.HIVE_TYPE_FLOAT.equalsIgnoreCase(columnTypesArray[i])) {
                    row.add(Double.valueOf(value.toString()).floatValue());
                } else if (SolrSerDe.HIVE_TYPE_DOUBLE.equalsIgnoreCase(columnTypesArray[i])) {
                    row.add(Double.valueOf(value.toString()));
                } else {
                    row.add(value.toString());
                }
                
            } else {
                row.add(null);
            }
        }
        return row;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return MapWritable.class;
    }

    @Override
    public Writable serialize(final Object obj, final ObjectInspector inspector)
            throws SerDeException {
        
        final StructObjectInspector structInspector = (StructObjectInspector) inspector;
        final List<? extends StructField> fields = structInspector
                .getAllStructFieldRefs();
        if (fields.size() != columnNames.size()) {
            throw new SerDeException(String.format(
                    "Required %d columns, received %d.", columnNames.size(),
                    fields.size()));
        }
        
        cachedWritable.clear();
        for (int c = 0; c < fieldCount; c++) {
            StructField structField = fields.get(c);
            if (structField != null) {
                final Object field = structInspector.getStructFieldData(obj,
                        structField);
                
                //TODO:currently only support hive primitive type
                final AbstractPrimitiveObjectInspector fieldOI = (AbstractPrimitiveObjectInspector)structField
                        .getFieldObjectInspector();

                Writable value = (Writable)fieldOI.getPrimitiveWritableObject(field);
                
                if (value == null) {
                    if(PrimitiveCategory.STRING.equals(fieldOI.getPrimitiveCategory())){
                        value = NullWritable.get(); 
                    }else{
                        value = new IntWritable(0);
                    }
                }
                cachedWritable.put(new Text(columnNames.get(c)), value);
            }
        }
        return cachedWritable;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return new SerDeStats();
    }
}