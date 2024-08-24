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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.deidentifier.arx.Data;
import org.deidentifier.arx.DataHandle;
import org.deidentifier.arx.DataHandleOutput;
import org.deidentifier.arx.aggregates.StatisticsFrequencyDistribution;

/**
 * Functions to handle commute categories
 */
public class Util {
	
	/**
	 * Helper class to draw from a distribution
	 */
	private static class Sampler {
		
		/** Values */
		private final String[] values;
		/** Frequencies */
		private final double[] frequencies;

		/**
		 * Constructor
		 * @param value1
		 * @param frequency1
		 * @param value2
		 * @param frequency2
		 */
		public Sampler(String value1, double frequency1, String value2, double frequency2) {
			
			// Init
			this.values = new String[] {value1, value2};
			this.frequencies = new double[] {frequency1, frequency2};

	        // Convert frequencies to cumulative frequencies
	        for (int i = 1; i < frequencies.length; i++) {
	            frequencies[i] += frequencies[i - 1];
	        }
	        
	        // Normalize
	        for (int i = 0; i < frequencies.length; i++) {
	        	frequencies[i] /= frequencies[frequencies.length - 1];
	        }
		}

		/**
		 * Draw from the distribution
		 * @return
		 */
		public String sample() {
			double r = RANDOM.nextDouble();
			for (int i = 0; i < frequencies.length; i++) {
				if (r <= frequencies[i]) {
					return values[i];
				}
			}
			throw new IllegalStateException("Random number out of bounds.");
		}
	}

    /** Seed*/
	private static long RANDOMSEED = 42l;
    /** RNG*/
    private static Random RANDOM = new Random(RANDOMSEED);
    /** Distribution */
    private Map<String, Double> commToSchDistribution = new HashMap<>();
    /** Distribution */
    private Map<String, Double> commFromSchDistribution = new HashMap<>();

    /**
     * Creates a new instance
     * @param input
     */
    public Util(DataHandle input) {

    	// Relevant columns
        int commToSchIndex = input.getColumnIndexOf(IO.FIELD_COMMUTE_TO_SCHOOL);
        int commFromSchIndex = input.getColumnIndexOf(IO.FIELD_COMMUTE_FROM_SCHOOL);
        
        // Get distributions
        StatisticsFrequencyDistribution commToSch = input.getStatistics().getFrequencyDistribution(commToSchIndex);
        StatisticsFrequencyDistribution commFromSch = input.getStatistics().getFrequencyDistribution(commFromSchIndex);
        
        // Store in maps
        for (int i = 0; i < commToSch.frequency.length; i++) {
        	commToSchDistribution.put(commToSch.values[i], commToSch.frequency[i]);
        }
        for (int i = 0; i < commFromSch.frequency.length; i++) {
        	commFromSchDistribution.put(commFromSch.values[i], commFromSch.frequency[i]);
        }
    }

    /**
     * Extract data
     * @param handle
     * @return
     */
    private List<String[]> getData(DataHandle handle) {

        // Prepare
        Iterator<String[]> iter = handle.iterator();
        List<String[]> rows = new ArrayList<>();
        rows.add(iter.next());
        int rowNumber = 0;

        // Convert
        while (iter.hasNext()) {
            String[] row = iter.next();
            if (!(handle instanceof DataHandleOutput) || !handle.isOutlier(rowNumber)) {
                rows.add(row);
            }
            rowNumber++;
        }

        // Done
        return rows;
    }

    /**
     * Applies the commute categories
     * @param handle
     * @return
     */
    public DataHandle reapplyCommuteCategories(DataHandle handle) {

    	// Relevant columns
        int commToSchIndex = handle.getColumnIndexOf(IO.FIELD_COMMUTE_TO_SCHOOL);
        int commFromSchIndex = handle.getColumnIndexOf(IO.FIELD_COMMUTE_FROM_SCHOOL);
        
        // Convert data
        List<String[]> data = getData(handle);
        
        // Map to recreate the original positions later on
        Map<String[], Integer> positions = new HashMap<String[], Integer>();
        int index = 0;
        for (String[] row : data) {
            positions.put(row, index);
            index++;
        }

        // Sort
        Collections.sort(data, new Comparator<String[]>() {
            @Override
            public int compare(String[] a, String[] b) {
                for (int i = 0; i < a.length; i++) {
                    int comparison = a[i].compareTo(b[i]);
                    if (comparison != 0) {
                        return comparison;
                    }
                }
                return 0;
            }
        });

        // For each group
        int current = 0;
        while (true) {
        	
            // Find group
            int start = current;
            int end = current;
            while (current < data.size() && Arrays.equals(data.get(start), data.get(current))) {
                current++;
            }
            end = current - 1;
            
            // Determine group properties
            String[] rowData = data.get(start);
            String commToSch = rowData[commToSchIndex];
            String commFromSch = rowData[commFromSchIndex];
            
            // Sample if combined group
            if (commToSch.contains(",")){
            	String[] values = commToSch.split(",");
            	String value1 = values[0].trim();
            	String value2 = values[1].trim();
            	commToSch = new Sampler(value1, commToSchDistribution.get(value1), 
            							value2, commToSchDistribution.get(value2)).sample();
            }
            // Sample if combined group
            if (commFromSch.contains(",")){
            	String[] values = commFromSch.split(",");
            	String value1 = values[0].trim();
            	String value2 = values[1].trim();
            	commFromSch = new Sampler(value1, commFromSchDistribution.get(value1), 
            							  value2, commFromSchDistribution.get(value2)).sample();
            }

            // Modify group
            for (int row = start; row <= end; row++) {
            	data.get(row)[commToSchIndex] = commToSch;
            	data.get(row)[commFromSchIndex] = commFromSch;
            }

            // Break
            if (current == data.size()) {
                break;
            }
        }
        
        // Recreate the original order
        data.sort(new Comparator<String[]>() {
            @Override
            public int compare(String[] o1, String[] o2) {
                return Integer.compare(positions.get(o1), positions.get(o2));
            }
        });
        
        // Return as handle
        return Data.create(data).getHandle();
    }
}
