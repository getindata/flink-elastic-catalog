package com.getindata.flink.connector.jdbc.catalog;

import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.junit.BeforeClass;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

class ElasticCatalogTestBase {
    protected static ElasticJdbcCatalogFactory catalogFactory;
    protected static String url;
    protected static final ElasticsearchTestContainer container = new ElasticsearchTestContainer();
    protected static final String USERNAME = "elastic";
    protected static final String PASSWORD = "password";

    private static final int REQUEST_RETRY_MAX_COUNT = 3;
    private static final int TIMEOUT_IN_SECONDS = 3;

    @BeforeClass
    public static void beforeAll() throws Exception {
        container.withEnv("xpack.security.enabled", "true");
        container.withEnv("ELASTIC_PASSWORD", PASSWORD);
        container.withEnv("ES_JAVA_OPTS", "-Xms1g -Xmx1g");
        container.start();
        TimeUnit.SECONDS.sleep(TIMEOUT_IN_SECONDS);
        if (!isTrialEnabled()) {
            enableTrial();
        }
        catalogFactory = new ElasticJdbcCatalogFactory();

        url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
    }

    private static void enableTrial() throws Exception {
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(String.format("http://%s:%d/_license/start_trial?acknowledge=true",
                        container.getHost(), container.getElasticPort()))
                .post(RequestBody.create(new byte[]{}))
                .addHeader("Authorization", Credentials.basic(USERNAME, PASSWORD))
                .build();
        try (Response response = client.newCall(request).execute()) {
            assertTrue(response.isSuccessful());
        }
    }

    private static boolean isTrialEnabled() throws Exception {
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(String.format("http://%s:%d/_license",
                        container.getHost(), container.getElasticPort()))
                .addHeader("Authorization", Credentials.basic(USERNAME, PASSWORD))
                .build();
        for (int i = 0; i < REQUEST_RETRY_MAX_COUNT; i++) {
            Response response = client.newCall(request).execute();
            int response_code = response.code();
            if (response_code == 200) {
                return response.body().string().contains("\"type\" : \"trial\"");
            } else if (response_code == 404) {
                TimeUnit.SECONDS.sleep(TIMEOUT_IN_SECONDS);
            } else {
                throw new CatalogException("Unexpected response retrieved from Elastic: " + response_code);
            }
        }
        throw new CatalogException("Ran out of retries trying to retrieve 200 status from Elastic!");
    }

    protected Map<String, String> getCommonOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("base-url", url);
        options.put("default-database", "test-database");
        options.put("password", PASSWORD);
        options.put("username", USERNAME);
        return options;
    }

    protected static void createTestIndex(String inputTable, String indexPath) throws Exception {
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(String.format("http://%s:%d/%s/", container.getHost(),
                        container.getElasticPort(), inputTable))
                .put(RequestBody.create(loadResource(indexPath)))
                .addHeader("Content-Type", "application/json")
                .addHeader("Authorization", Credentials.basic(USERNAME, PASSWORD))
                .build();
        try (Response response = client.newCall(request).execute()) {
            assertTrue(response.isSuccessful());
        }
    }

    protected static void addTestData(String inputTable, String inputPath) throws Exception {
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(String.format("http://%s:%d/%s/_bulk/", container.getHost(),
                        container.getElasticPort(), inputTable))
                .post(RequestBody.create(loadResource(inputPath)))
                .addHeader("Content-Type", "application/json")
                .addHeader("Authorization", Credentials.basic(USERNAME, PASSWORD))
                .build();
        client.newCall(request).execute();
    }

    protected static byte[] loadResource(String path) throws IOException {
        return IOUtils.toByteArray(
                Objects.requireNonNull(ElasticCatalogTest.class.getClassLoader().getResourceAsStream(path))
        );
    }

    protected static String calculateExpectedTemporalLowerBound() {
        // upper bound for temporal partition columns is the last milisecond of the current day
        LocalDate todayDate = LocalDate.now();
        return String.valueOf(todayDate.atTime(LocalTime.MIN).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
    }

    protected static String calculateExpectedTemporalUpperBound() {
        // upper bound for temporal partition columns is the last milisecond of the current day
        LocalDate tomorrowDate = LocalDate.now().plusDays(1);
        return String.valueOf(tomorrowDate.atTime(LocalTime.MIDNIGHT).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() - 1);
    }
}
