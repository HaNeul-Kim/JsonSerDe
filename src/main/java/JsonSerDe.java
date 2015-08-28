import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.*;

/**
 * Test JsonSerDe.
 *
 * @author Ha Neul, Kim
 */
public class JsonSerDe extends AbstractSerDe {

    private String characterSet;

    private List<String> columnNames;

    private List<TypeInfo> columnTypes;

    private SerDeStats stats;

    private StandardStructObjectInspector rowObjectInspector;

    /**
     * create table 시 호출
     *
     * @param conf  설정
     * @param table 테이블
     * @throws SerDeException
     */
    @Override
    public void initialize(Configuration conf, Properties table) throws SerDeException {
        String columnNameProperty = table.getProperty(serdeConstants.LIST_COLUMNS);
        String columnTypeProperty = table.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        columnNames = Arrays.asList(columnNameProperty.split(","));
        int columnsLength = columnNames.size();
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        List<ObjectInspector> columnOIs = new ArrayList<>();
        // ci = column index
        for (int ci = 0; ci < columnsLength; ci++) {
            TypeInfo colTypeInfo = columnTypes.get(ci);
            columnOIs.add(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(colTypeInfo));
        }

        rowObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);

        stats = new SerDeStats();

        this.characterSet = table.getProperty("characterSet");
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
        return null;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return this.stats;
    }

    /**
     * select 시 호출
     *
     * @param blob 입력받는 Writable
     * @return 최종 보여지게 될 JavaObject
     * @throws SerDeException
     */
    @Override
    public Object deserialize(Writable blob) throws SerDeException {
        List<Writable> writables = new ArrayList<>();

        try {
            ObjectMapper mapper = new ObjectMapper();
            Map map = mapper.readValue(blob.toString(), Map.class);

            writables.add(new IntWritable(Integer.parseInt(map.get("number").toString())));
            writables.add(new Text(map.get("text").toString()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return writables;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return this.rowObjectInspector;
    }
}