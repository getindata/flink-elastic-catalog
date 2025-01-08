package com.getindata.flink.connector.jdbc.catalog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

class IndexFilterResolver {

    private static final Logger LOG = LoggerFactory.getLogger(IndexFilterResolver.class);

    static IndexFilterResolver acceptAll() {
        return new IndexFilterResolver(emptyList(), emptyList());
    }

    static IndexFilterResolver of(String rawIncludePatterns, String rawExcludePatterns) {
        return new IndexFilterResolver(parseRaw(rawExcludePatterns), parseRaw(rawIncludePatterns));
    }

    private static List<Pattern> parseRaw(String commaSeparatedList) {
        if (commaSeparatedList == null || commaSeparatedList.isEmpty()) {
            return emptyList();
        }
        return Arrays.stream(commaSeparatedList.split(","))
                .map(String::trim)
                .map(Pattern::compile)
                .collect(Collectors.toList());
    }

    private final List<Pattern> excludePatterns;
    private final List<Pattern> includePatterns;

    IndexFilterResolver(List<Pattern> excludePatterns, List<Pattern> includePatterns) {
        this.excludePatterns = excludePatterns;
        this.includePatterns = includePatterns;
    }

    boolean isAccepted(String objectName) {
        if (!includePatterns.isEmpty()) {
            if (includePatterns.stream().noneMatch(pattern -> pattern.matcher(objectName).matches())) {
                LOG.debug("'{}' does not match any include pattern. Include patterns='{}'.", objectName, includePatterns);
                return false;
            }
        }
        if (!excludePatterns.isEmpty()) {
            if (excludePatterns.stream().anyMatch(pattern -> pattern.matcher(objectName).matches())) {
                LOG.debug("'{}' matches exclude pattern; exclude patterns='{}'.", objectName, excludePatterns);
                return false;
            }
        }
        LOG.debug("'{}' matches include pattern and does not match any exclude pattern.", objectName);
        return true;
    }

}
