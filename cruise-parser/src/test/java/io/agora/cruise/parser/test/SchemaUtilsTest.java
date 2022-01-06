package io.agora.cruise.parser.test;

import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;

/** SchemaToolTest. */
public class SchemaUtilsTest extends TestBase {

    public SchemaUtilsTest() {}

    @Test
    public void test() {

        Assert.assertNotNull(
                Objects.requireNonNull(rootSchema.getSubSchema("test_db")).getTable("test_table"));
        Assert.assertNotNull(
                Objects.requireNonNull(rootSchema.getSubSchema("test_db2"))
                        .getTable("test_table2"));
    }
}
