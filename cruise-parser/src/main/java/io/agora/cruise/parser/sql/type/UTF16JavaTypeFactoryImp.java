package io.agora.cruise.parser.sql.type;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/** UTF16JavaTypeFactoryImp. */
public class UTF16JavaTypeFactoryImp extends JavaTypeFactoryImpl {

    @Override
    public Charset getDefaultCharset() {
        return StandardCharsets.UTF_16;
    }
}
