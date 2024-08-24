/**
 * Anonymization process for the commute health study
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bihmi.commute;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import org.deidentifier.arx.ARXAnonymizer;
import org.deidentifier.arx.ARXConfiguration;
import org.deidentifier.arx.ARXListener;
import org.deidentifier.arx.ARXProcessStatistics;
import org.deidentifier.arx.ARXResult;
import org.deidentifier.arx.AttributeType;
import org.deidentifier.arx.Data;
import org.deidentifier.arx.DataHandle;
import org.deidentifier.arx.DataType;
import org.deidentifier.arx.aggregates.HierarchyBuilderIntervalBased;
import org.deidentifier.arx.criteria.KAnonymity;
import org.deidentifier.arx.exceptions.RollbackRequiredException;
import org.deidentifier.arx.metric.Metric;

/**
 * Implements the anonymization process
 */
public class Anon {

    /**
     * Main anonymization process
     *
     * @param data
     * @return
     * @throws IOException
     */
    public static DataHandle anonymizeCommuteData(Data data) throws IOException {
    	
        // Specify transformation rules
        data.getDefinition().setAttributeType(IO.FIELD_COMMUTE_FROM_SCHOOL, getCommuteHierarchy(data));
        data.getDefinition().setMaximumGeneralization(IO.FIELD_COMMUTE_FROM_SCHOOL, 2);
        data.getDefinition().setAttributeType(IO.FIELD_COMMUTE_TO_SCHOOL, getCommuteHierarchy(data));
        data.getDefinition().setMaximumGeneralization(IO.FIELD_COMMUTE_TO_SCHOOL, 2);
        data.getDefinition().setAttributeType(IO.FIELD_DISTANCE_TO_SCHOOL, getDistToSchoolHierarchy(data));
        data.getDefinition().setMicroAggregationFunction(IO.FIELD_DISTANCE_TO_SCHOOL, AttributeType.MicroAggregationFunction.createArithmeticMean(), true);
        data.getDefinition().setAttributeType(IO.FIELD_DISTANCE_FROM_SCHOOL, getDistFromSchoolHierarchy(data));
        data.getDefinition().setMicroAggregationFunction(IO.FIELD_DISTANCE_FROM_SCHOOL, AttributeType.MicroAggregationFunction.createArithmeticMean(), true);
        data.getDefinition().setAttributeType(IO.FIELD_MVPA_SQRT, getMVPAHierarchy(data));
        data.getDefinition().setMicroAggregationFunction(IO.FIELD_MVPA_SQRT, AttributeType.MicroAggregationFunction.createArithmeticMean(), true);
        data.getDefinition().setAttributeType(IO.FIELD_VO2_MAX, getVO2MaxHierarchy(data));
        data.getDefinition().setMicroAggregationFunction(IO.FIELD_VO2_MAX, AttributeType.MicroAggregationFunction.createArithmeticMean(), true);
        data.getDefinition().setAttributeType(IO.FIELD_AGE, getAgeHierarchy(data));
        data.getDefinition().setMicroAggregationFunction(IO.FIELD_AGE, AttributeType.MicroAggregationFunction.createArithmeticMean(), true);
        data.getDefinition().setAttributeType(IO.FIELD_GENDER, getGenderHierarchy());
        data.getDefinition().setMaximumGeneralization(IO.FIELD_GENDER, 0);
    	
        // Prepare config
        ARXConfiguration config = ARXConfiguration.create();

        // Configure transformation model
        config.setSuppressionLimit(1d);
        config.addPrivacyModel(new KAnonymity(2));
        config.setQualityModel(Metric.createLossMetric(0, Metric.AggregateFunction.GEOMETRIC_MEAN));
        config.setAlgorithm(ARXConfiguration.AnonymizationAlgorithm.BEST_EFFORT_BOTTOM_UP);
        config.setHeuristicSearchTimeLimit(30000);
        
        // Status
        System.out.println("Preparations completed");
        
        // Timer
        Instant start = Instant.now();

        // Anonymize
        ARXAnonymizer anonymizer = new ARXAnonymizer();
        ARXResult result = anonymizer.anonymize(data, config);
        
        // Status
        System.out.println("Initial anonymization performed");

        // Optimize
        DataHandle output = result.getOutput();
        ARXProcessStatistics statistics = result.getProcessStatistics();
        double oMin = 1d / 100d;
        try {
        	statistics = statistics.merge(result.optimizeIterativeFast(output, oMin, new ARXListener() {
            	int progress = -1;
				@Override
				public void progress(double arg0) {
					int current = (int)(Math.round(arg0 * 100d));
					if (current != progress) {
						progress = current;
						System.out.println("Optimizing. Progress: " + progress + "%");
					}
				}
			}));
        } catch (RollbackRequiredException e) {
            throw new RuntimeException(e);
        }

        // Make sure to output fine-grained commute categories
        Util util = new Util(result.getInput());
        output = util.reapplyCommuteCategories(output);
        
        // Timer
        Instant end = Instant.now();
        Duration timeElapsed = Duration.between(start, end);
        LocalTime time = LocalTime.ofSecondOfDay(timeElapsed.getSeconds()).withNano(timeElapsed.getNano());
        System.out.println("Time taken: " + time.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")));
        
        // Status
        System.out.println("Transformation schemes applied: " + statistics.getNumberOfSteps());
        
        // Done
        return output;
    }

    /**
     * Age hierarchy
     *
     * @return
     */
    private static AttributeType.Hierarchy getAgeHierarchy(Data data) {
    	
    	// Specs
        double minAge = 8d;
        double maxAge =  15.5d;
        
        // Init
        HierarchyBuilderIntervalBased<Double> hierarchyBuilder = HierarchyBuilderIntervalBased.create(
                DataType.DECIMAL,
                new HierarchyBuilderIntervalBased.Range<Double>(minAge, minAge, minAge),
                new HierarchyBuilderIntervalBased.Range<Double>(maxAge, maxAge, maxAge));

        // Define base intervals
        hierarchyBuilder.setAggregateFunction(DataType.DECIMAL.createAggregate().createArithmeticMeanFunction());
        hierarchyBuilder.addInterval(minAge, minAge+.25d);
        
        // Define grouping
        hierarchyBuilder.getLevel(0).addGroup(2);
        hierarchyBuilder.getLevel(1).addGroup(2);

        // Prepare and return
        hierarchyBuilder.prepare(data.getHandle().getDistinctValues(data.getHandle().getColumnIndexOf(IO.FIELD_AGE)));
        return hierarchyBuilder.build();
    }

    /**
     * Gender hierarchy
     *
     * @return
     */
    private static AttributeType.Hierarchy getGenderHierarchy() {
        AttributeType.Hierarchy.DefaultHierarchy hierarchy = AttributeType.Hierarchy.create();
        hierarchy.add("female");
        hierarchy.add("male");
        return hierarchy;
    }

    /**
     * VO2Max hierarchy
     * @param data
     * @return
     */
    private static AttributeType.Hierarchy getVO2MaxHierarchy(Data data) {
    	
    	// Specs
        double minValue = 30d;
        double maxValue =  65d;
        double intervalRange = 2.5d;

        // Init
        HierarchyBuilderIntervalBased<Double> hierarchyBuilder = HierarchyBuilderIntervalBased.create(
                DataType.DECIMAL,
                new HierarchyBuilderIntervalBased.Range<Double>(minValue, minValue, minValue),
                new HierarchyBuilderIntervalBased.Range<Double>(maxValue, maxValue, maxValue));

        // Define base intervals
        hierarchyBuilder.setAggregateFunction(DataType.DECIMAL.createAggregate().createArithmeticMeanFunction());
        hierarchyBuilder.addInterval(minValue, minValue + intervalRange);
        
        // Define grouping
        hierarchyBuilder.getLevel(0).addGroup(2);
        hierarchyBuilder.getLevel(1).addGroup(5);

        // Prepare and return
        hierarchyBuilder.prepare(data.getHandle().getDistinctValues(data.getHandle().getColumnIndexOf(IO.FIELD_VO2_MAX)));
        return hierarchyBuilder.build();
    }

    /**
     * MVPA hierarchy
     * @param data
     * @return
     */
    private static AttributeType.Hierarchy getMVPAHierarchy(Data data) {
    	
    	// Specs
        double minValue = 8d;
        double maxValue =  50d;
        double intervalRange = .5d;

        // Init
        HierarchyBuilderIntervalBased<Double> hierarchyBuilder = HierarchyBuilderIntervalBased.create(
                DataType.DECIMAL,
                new HierarchyBuilderIntervalBased.Range<Double>(minValue, minValue, minValue),
                new HierarchyBuilderIntervalBased.Range<Double>(maxValue, maxValue, maxValue));

        // Define base intervals
        hierarchyBuilder.setAggregateFunction(DataType.DECIMAL.createAggregate().createArithmeticMeanFunction());
        hierarchyBuilder.addInterval(minValue, minValue + intervalRange);
        
        // Define grouping
        hierarchyBuilder.getLevel(0).addGroup(2);
        hierarchyBuilder.getLevel(1).addGroup(2);
        hierarchyBuilder.getLevel(2).addGroup(2);
        hierarchyBuilder.getLevel(3).addGroup(2);

        // Prepare and return
        hierarchyBuilder.prepare(data.getHandle().getDistinctValues(data.getHandle().getColumnIndexOf(IO.FIELD_MVPA_SQRT)));
        return hierarchyBuilder.build();
    }

    /**
     * Dist to school hierarchy
     * @param data
     * @return
     */
    private static AttributeType.Hierarchy getDistToSchoolHierarchy(Data data) {
    	
    	// Specs
        double minValue = 0d;
        double maxValue =  22500d;
        double intervalRange = 2d;

        // Init
        HierarchyBuilderIntervalBased<Double> hierarchyBuilder = HierarchyBuilderIntervalBased.create(
                DataType.DECIMAL,
                new HierarchyBuilderIntervalBased.Range<Double>(minValue, minValue, minValue),
                new HierarchyBuilderIntervalBased.Range<Double>(maxValue, maxValue, maxValue));

        // Define base intervals
        hierarchyBuilder.setAggregateFunction(DataType.DECIMAL.createAggregate().createArithmeticMeanFunction());
        hierarchyBuilder.addInterval(minValue, minValue + intervalRange);
        
        // Define grouping
        hierarchyBuilder.getLevel(0).addGroup(2);
        hierarchyBuilder.getLevel(1).addGroup(5);
        hierarchyBuilder.getLevel(2).addGroup(5);
        hierarchyBuilder.getLevel(3).addGroup(5);

        // Prepare and return
        hierarchyBuilder.prepare(data.getHandle().getDistinctValues(data.getHandle().getColumnIndexOf(IO.FIELD_DISTANCE_TO_SCHOOL)));
        return hierarchyBuilder.build();
    }

    /**
     * Dist from school hierarchy
     * @param data
     * @return
     */
    private static AttributeType.Hierarchy getDistFromSchoolHierarchy(Data data) {
    	
    	// Specs
        double minValue = 130d;
        double maxValue =  22270d;
        double intervalRange = 2d;

        // Init
        HierarchyBuilderIntervalBased<Double> hierarchyBuilder = HierarchyBuilderIntervalBased.create(
                DataType.DECIMAL,
                new HierarchyBuilderIntervalBased.Range<Double>(minValue, minValue, minValue),
                new HierarchyBuilderIntervalBased.Range<Double>(maxValue, maxValue, maxValue));

        // Define base intervals
        hierarchyBuilder.setAggregateFunction(DataType.DECIMAL.createAggregate().createArithmeticMeanFunction());
        hierarchyBuilder.addInterval(minValue, minValue + intervalRange);
        
        // Define grouping
        hierarchyBuilder.getLevel(0).addGroup(2);
        hierarchyBuilder.getLevel(1).addGroup(5);
        hierarchyBuilder.getLevel(2).addGroup(5);
        hierarchyBuilder.getLevel(3).addGroup(5);

        // Prepare and return
        hierarchyBuilder.prepare(data.getHandle().getDistinctValues(data.getHandle().getColumnIndexOf(IO.FIELD_DISTANCE_FROM_SCHOOL)));
        return hierarchyBuilder.build();
    }

    /**
     * Commute hierarchy
     * @param data
     * @return
     */
    private static AttributeType.Hierarchy getCommuteHierarchy(Data data) {
        AttributeType.Hierarchy.DefaultHierarchy hierarchy = AttributeType.Hierarchy.create();
        hierarchy.add("car", "car", "car,public", "*");
        hierarchy.add("public", "public", "car,public", "*");
        hierarchy.add("walk", "walk, wheels", "walk,wheels", "*");
        hierarchy.add("wheels", "walk, wheels", "walk,wheels", "*");
        return hierarchy;
    }
}
