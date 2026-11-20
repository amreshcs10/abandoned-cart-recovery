package com.example.cartrecovery;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a fake-clock scenario. Each scenario is written once and run two ways: by
 * {@link PipelineTests} (JDK only, used by run.sh / run.cmd) and by {@link PipelineJUnitTest}
 * (JUnit 5, used by mvn test).
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Scenario {}
