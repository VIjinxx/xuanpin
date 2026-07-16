package com.lingyun.business.common.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JsonUtilTest {

    @Test
    public void parseJsonSupportsPythonStyleLiteralWhenStrictJsonFails() {
        JsonNode node = JsonUtil.parseJson("{"
                + "'empty': None,"
                + "'enabled': True,"
                + "'disabled': False,"
                + "'text': 'None True False should stay text',"
                + "'nested': {'value': 7}"
                + "}");

        assertNotNull(node);
        assertTrue(node.get("empty").isNull());
        assertTrue(node.get("enabled").asBoolean());
        assertFalse(node.get("disabled").asBoolean());
        assertEquals("None True False should stay text", node.get("text").asText());
        assertEquals(7, node.path("nested").path("value").asInt());
    }

    @Test
    public void parseJsonReturnsNullForInvalidInput() {
        assertNull(JsonUtil.parseJson("{'empty': None"));
    }
}
