package com.getindata.flink.connector.jdbc.catalog;

import org.apache.flink.shaded.guava30.com.google.common.base.Strings;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

class IndexFilterResolver {

    static IndexFilterResolver acceptAll() {
        return new IndexFilterResolver(emptyList(), emptyList());
    }

    static IndexFilterResolver of(String rawIncludePatterns, String rawExcludePatterns) {
        return new IndexFilterResolver(parseRaw(rawExcludePatterns), parseRaw(rawIncludePatterns));
    }

    private static List<Pattern> parseRaw(String commaSeparatedList) {
        if (Strings.isNullOrEmpty(commaSeparatedList)) {
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
                return false;
            }
        }
        if (!excludePatterns.isEmpty()) {
            return excludePatterns.stream().noneMatch(pattern -> pattern.matcher(objectName).matches());
        }
        return true;
    }

}
