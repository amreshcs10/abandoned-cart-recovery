package com.example.cartrecovery;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.stream.Stream;

/** Runs every {@link Scenario} under JUnit 5 (mvn test), one dynamic test per scenario. */
class PipelineJUnitTest {

    @TestFactory
    Stream<DynamicTest> scenarios() {
        return PipelineTests.cases().stream()
                .map(c -> DynamicTest.dynamicTest(c.name(), c.body()::run));
    }
}
