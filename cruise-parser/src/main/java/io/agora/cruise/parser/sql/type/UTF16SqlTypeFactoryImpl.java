package io.agora.cruise.parser.sql.type;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/** UDF16SqlTypeFactoryImpl. */
public class UTF16SqlTypeFactoryImpl extends SqlTypeFactoryImpl {

    public UTF16SqlTypeFactoryImpl() {
        super(RelDataTypeSystem.DEFAULT);
    }

    public UTF16SqlTypeFactoryImpl(RelDataTypeSystem typeSystem) {
        super(typeSystem);
    }

    @Override
    public Charset getDefaultCharset() {
        return StandardCharsets.UTF_16;
    }
}
