package com.capinfo.kafka.connector.dialect;

import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.fail;

/**
 * @author zhanghaonan
 * @date 2022/2/9
 */
public class DatabaseDialectsTest {

    @Test
    public void testLoadAllDialects() {
        Collection<DatabaseDialectProvider> providers = DatabaseDialects.registeredDialectProviders();
        assertContainsInstanceOf(providers, MySqlDatabaseDialect.Provider.class);
    }

    private void assertContainsInstanceOf(
            Collection<? extends DatabaseDialectProvider> providers,
            Class<? extends DatabaseDialectProvider> clazz
    ) {
        for (DatabaseDialectProvider provider : providers) {
            if (provider.getClass().equals(clazz))
                return;
        }
        fail("Missing " + clazz.getName());
    }

}
