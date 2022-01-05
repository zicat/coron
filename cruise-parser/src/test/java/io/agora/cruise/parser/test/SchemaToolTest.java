package io.agora.cruise.parser.test;

import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;

/** SchemaToolTest. */
public class SchemaToolTest extends TestBase {

    public SchemaToolTest() {}

    @Test
    public void test() {

        Assert.assertNotNull(
                Objects.requireNonNull(rootSchema.getSubSchema("test_db")).getTable("test_table"));
        Assert.assertNotNull(
                Objects.requireNonNull(rootSchema.getSubSchema("test_db2"))
                        .getTable("test_table2"));
    }
}
