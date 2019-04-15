package prv.saevel.bigdata.sql.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class UniqueIdUDAF extends UDAF {

    public static class UniqueIdUDAFEvaluator implements UDAFEvaluator {

        public static class State {

            public State(boolean isUnique, Set<Long> values) {
                this.isUnique = isUnique;
                this.values = values;
            }

            boolean isUnique;


            Set<Long> values;

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                State state = (State) o;
                return isUnique == state.isUnique &&
                        values.equals(state.values);
            }

            @Override
            public int hashCode() {
                return Objects.hash(isUnique, values);
            }
        }

        State state;

        public UniqueIdUDAFEvaluator() {
            super();
            init();
        }

        /**
         * Called initially for each partition. Initializes the computatinal state.
         */
        @Override
        public void init() {

        }

        /**
         * function: iterate
         * This function is called for every individual record of a group
         */

        public boolean iterate(long value){
            return true;
        }

        /**
         * function: terminatePartial
         * this function is called on the mapper side and
         * returns partially aggregated results.
         */
        public State terminatePartial(){
            return null;
        }

        /**
         * function: merge
         * This function is called two merge two partially aggregated resultsn
         */
        public boolean merge(State another){
            return true;
        }

        /**
         * function: terminate
         * this function is called after the last record of the group has been streamed
         */
        public boolean terminate(){
            return false;
        }
    }
}
