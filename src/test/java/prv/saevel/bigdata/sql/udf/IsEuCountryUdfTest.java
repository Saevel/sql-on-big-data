package prv.saevel.bigdata.sql.udf;


import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import static org.junit.jupiter.api.DynamicTest.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import prv.saevel.bigdata.sql.EuropeanUnion;

import static  org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IsEuCountryUdfTest {

    @TestFactory
    public List<DynamicTest> shouldReturnTrueForAllEuCountries() {
        final IsEuCountryUdf udf = new IsEuCountryUdf();
        return EuropeanUnion
                .members
                .stream()
                .map(toPositiveCase(udf))
                .collect(Collectors.toList());
    }

    @TestFactory
    public List<DynamicTest> shouldReturnFalseForRandomStrings() {
        final IsEuCountryUdf udf = new IsEuCountryUdf();
        final Random randomizer = new Random();

        return Stream.generate(randomStrings(randomizer))
                .map(toNegativeCase(udf))
                .limit(20)
                .collect(Collectors.toList());
    }

    private Supplier<String> randomStrings(final Random random){
        return () -> RandomStringUtils.randomAlphanumeric(random.nextInt(20));
    }

    private Function<String, DynamicTest> toNegativeCase(final IsEuCountryUdf udf){
        return country -> dynamicTest(
                "Should return false for " + country + ".",
                () -> {
                    BooleanWritable result = udf.evaluate(new Text(country));
                    assertNotNull(result);
                    assertFalse(result.get());
                });
    }

    private Function<String, DynamicTest> toPositiveCase(final IsEuCountryUdf udf){
        return country -> dynamicTest(
                "Should return true for " + country + ".",
                () -> {
                    BooleanWritable result = udf.evaluate(new Text(country));
                    assertNotNull(result);
                    assertTrue(result.get());
                });
    }
}
