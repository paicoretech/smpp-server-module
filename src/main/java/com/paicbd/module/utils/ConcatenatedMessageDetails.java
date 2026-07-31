package com.paicbd.module.utils;

public record ConcatenatedMessageDetails(
        int msgReferenceNumber,
        int totalSegments,
        int segmentSequence,
        String text
) {
}
