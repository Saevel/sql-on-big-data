package prv.saevel.bigdata.sql.udaf;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.*;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UniqueIdUDAFTest {

    @Test
    public void shouldInitializeIsUniqueToTrue(){
        UniqueIdUDAF.UniqueIdUDAFEvaluator udaf = new UniqueIdUDAF.UniqueIdUDAFEvaluator();
        udaf.init();
        assertNotNull(udaf.state, "The internal state of the UDAF should not be null");
        assertTrue(udaf.state.isUnique, "The 'is unique' field of the UDAF state should initially be true");
    }

    @Test
    public void shouldInitializeValuesToBeEmpty(){
        UniqueIdUDAF.UniqueIdUDAFEvaluator udaf = new UniqueIdUDAF.UniqueIdUDAFEvaluator();
        udaf.init();
        assertNotNull(udaf.state, "The internal state of the UDAF should not be null");
        assertNotNull(udaf.state.values, "The values list of the UDAF should not be null");
        assertTrue(udaf.state.values.isEmpty(), "The values list of the UDAF should initially be empty");
    }

    @TestFactory
    List<DynamicTest> shouldSetIsUniqueToFalseIfTheValueIsAlreadyOnTheList(){
        UniqueIdUDAF.UniqueIdUDAFEvaluator udaf = new UniqueIdUDAF.UniqueIdUDAFEvaluator();
        udaf.init();
        final Random randomizer = new Random();
        return Stream.generate(randomValueLists(randomizer))
                .map(toNegativeValueListCase(randomizer, udaf))
                .limit(20)
                .collect(Collectors.toList());

    }

    @TestFactory
    List<DynamicTest> shouldSetIsUniqueToFalseIfTheValueIsNotOntheList(){
        UniqueIdUDAF.UniqueIdUDAFEvaluator udaf = new UniqueIdUDAF.UniqueIdUDAFEvaluator();
        udaf.init();
        final Random randomizer = new Random();
        return Stream.generate(randomValueLists(randomizer))
                .map(toPositiveValueListCase(randomizer, udaf))
                .limit(20)
                .collect(Collectors.toList());
    }

    @TestFactory
    List<DynamicTest> terminatePartialShouldAlwaysReturnTheCurrentLocalState(){
        UniqueIdUDAF.UniqueIdUDAFEvaluator udaf = new UniqueIdUDAF.UniqueIdUDAFEvaluator();
        udaf.init();
        final Random randomizer = new Random();
        return Stream.generate(randomValueLists(randomizer))
                .map(toTerminateParialCase(randomizer, udaf))
                .limit(20)
                .collect(Collectors.toList());
    }

    @TestFactory
    List<DynamicTest> shouldFlipUniquenessWhenMerginigWithANonUniqueState(){
        UniqueIdUDAF.UniqueIdUDAFEvaluator udaf = new UniqueIdUDAF.UniqueIdUDAFEvaluator();
        udaf.init();
        final Random randomizer = new Random();
        return Stream.generate(randomValueLists(randomizer))
                .map(toMergeWithNonUniqueState(randomizer, udaf))
                .limit(20)
                .collect(Collectors.toList());
    }

    @TestFactory
    List<DynamicTest> shouldChangeUniquenessToFalseInCaseOfOverlappingValues(){
        UniqueIdUDAF.UniqueIdUDAFEvaluator udaf = new UniqueIdUDAF.UniqueIdUDAFEvaluator();
        udaf.init();
        final Random randomizer = new Random();
        return Stream.generate(randomValueLists(randomizer))
                .map(toMergeWithStateWithOverlappingValues(randomizer, udaf))
                .limit(20)
                .collect(Collectors.toList());
    }

    @TestFactory
    List<DynamicTest> terminateShouldAlwaysReturnTheFinalUniqueness(){
        UniqueIdUDAF.UniqueIdUDAFEvaluator udaf = new UniqueIdUDAF.UniqueIdUDAFEvaluator();
        udaf.init();
        final Random randomizer = new Random();
        return Stream.generate(randomValueLists(randomizer))
                .map(toTerminateFinalCase(randomizer, udaf))
                .limit(20)
                .collect(Collectors.toList());
    }

    private Function<List<Long>, DynamicTest> toMergeWithNonUniqueState(final Random randomizer,
                                                                        final UniqueIdUDAF.UniqueIdUDAFEvaluator udaf){
       return values -> {
           final boolean initialUniqueness = randomizer.nextBoolean();
           final List<Long> otherValues = randomValueLists(randomizer).get();
           return dynamicTest(
                   "For the initial uniqueness: " + initialUniqueness + " and any values non the list," +
                           " merging with an already non-unique partial should result in uniqueness of: false",
                   () -> {
                       final UniqueIdUDAF.UniqueIdUDAFEvaluator.State initialState = new UniqueIdUDAF.UniqueIdUDAFEvaluator.State(initialUniqueness, values);
                       final UniqueIdUDAF.UniqueIdUDAFEvaluator.State mergedState = new UniqueIdUDAF.UniqueIdUDAFEvaluator.State(false, otherValues);
                       udaf.state = initialState;
                       udaf.merge(mergedState);
                       assertNotNull(udaf.state, "The internal state of the UDAF should not be null");
                       assertFalse(udaf.state.isUnique, "The 'is unique' field of the UDAF state should initially be false");
                   }
           );
       };
    }

    private Function<List<Long>, DynamicTest> toMergeWithStateWithOverlappingValues(final Random randomizer,
                                                                                    final UniqueIdUDAF.UniqueIdUDAFEvaluator udaf){
        return values -> {
            final boolean initialUniqueness = randomizer.nextBoolean();
            final boolean secondUniqueness = randomizer.nextBoolean();
            final List<Long> otherValues = Arrays.asList(values.get(randomizer.nextInt(values.size())));
            return dynamicTest(
                    "For any initial(" + initialUniqueness + ") and merged (" + secondUniqueness +
                            ") uniqueness, if the values on the two lists overlap(" + otherValues + "), the result " +
                            "should have the uniqueness of: false.",
                    () -> {
                        final UniqueIdUDAF.UniqueIdUDAFEvaluator.State initialState = new UniqueIdUDAF.UniqueIdUDAFEvaluator.State(initialUniqueness, values);
                        final UniqueIdUDAF.UniqueIdUDAFEvaluator.State mergedState = new UniqueIdUDAF.UniqueIdUDAFEvaluator.State(secondUniqueness, otherValues);
                        udaf.state = initialState;
                        udaf.merge(mergedState);
                        assertNotNull(udaf.state, "The internal state of the UDAF should not be null");
                        assertFalse(udaf.state.isUnique, "The 'is unique' field of the UDAF state should initially be false");
                    }
            );
        };
    }

    private Function<List<Long>, DynamicTest> toTerminateParialCase(final Random randomizer,
                                                                    final UniqueIdUDAF.UniqueIdUDAFEvaluator udaf){
        return values -> {
            final boolean uniqueness = randomizer.nextBoolean();
            return dynamicTest("Should return the list: " + values + " and uniqueness: " + uniqueness +
                    " when it is the local state of the partition",
                    () -> {
                        final UniqueIdUDAF.UniqueIdUDAFEvaluator.State state = new UniqueIdUDAF.UniqueIdUDAFEvaluator.State(uniqueness, values);
                        udaf.state = state;
                        final UniqueIdUDAF.UniqueIdUDAFEvaluator.State returnedState = udaf.terminatePartial();
                        assertNotNull(returnedState, "The internal state of the UDAF should not be null");
                        assertEquals(state, returnedState, "The internal state of the UDAF shuld be returned");
                    });
        };
    }

    private Function<List<Long>, DynamicTest> toTerminateFinalCase(final Random randomizer,
                                                                    final UniqueIdUDAF.UniqueIdUDAFEvaluator udaf){
        return values -> {
            final boolean uniqueness = randomizer.nextBoolean();
            return dynamicTest("Should return the final uniqueness inside the UDAF: " + uniqueness + ".",
                    () -> {
                        final UniqueIdUDAF.UniqueIdUDAFEvaluator.State state = new UniqueIdUDAF.UniqueIdUDAFEvaluator.State(uniqueness, values);
                        udaf.state = state;
                        final boolean returnedState = udaf.terminate();
                        assertEquals(uniqueness, returnedState, "Terminate should returned the uniqueness finally" +
                                "in the UDAF, inrregardless of other parameters.");
                    });
        };
    }

    private Function<List<Long>, DynamicTest> toPositiveValueListCase(final Random randomizer,
                                                                     final UniqueIdUDAF.UniqueIdUDAFEvaluator udaf){
        return values -> {
            final Long chosenValue = generateAbsent(values, randomizer);
            return dynamicTest(
                    "Should leave uniqueness as true for " + chosenValue.longValue() + "given that it is not " +
                            "on the value list",
                    () -> {
                        udaf.state.values.addAll(values);
                        udaf.iterate(chosenValue);
                        assertNotNull(udaf.state, "The internal state of the UDAF should not be null");
                        assertNotNull(udaf.state.values, "The values list of the UDAF should not be null");
                        assertTrue(udaf.state.isUnique, "The uniqueness in the UDAF internal state should remain true");
                    }
            );
        };
    }

    private long generateAbsent(final List<Long> values, final Random randomizer) {
        long generated = randomizer.nextLong();
        if(values.contains(generated)){
            return generateAbsent(values, randomizer);
        } else {
            return generated;
        }
    }

    private Function<List<Long>, DynamicTest> toNegativeValueListCase(final Random randomizer,
                                                                      final UniqueIdUDAF.UniqueIdUDAFEvaluator udaf){
        return values -> {
            final Long chosenValue = values.get(randomizer.nextInt(values.size()));
            return dynamicTest(
                    "Should change uniqueness to false for " + chosenValue.longValue() + "given that it already" +
                            "is on the value list",
                    () -> {
                        udaf.state.values.addAll(values);
                        udaf.iterate(chosenValue);
                        assertNotNull(udaf.state, "The internal state of the UDAF should not be null");
                        assertNotNull(udaf.state.values, "The values list of the UDAF should not be null");
                        assertFalse(udaf.state.isUnique, "The uniqueness in the UDAF internal state should flip to false");
                    }
            );
        };
    }

    private Supplier<List<Long>> randomValueLists(final Random randomizer) {
        return () -> randomizer.longs().limit(randomizer.nextInt(19) + 1).boxed().collect(Collectors.toList());
    }
}
