package prv.saevel.bigdata.sql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

public class IsEuCountryUdf extends UDF {

    public BooleanWritable evaluate(Text name){
        return null;
    }
}
